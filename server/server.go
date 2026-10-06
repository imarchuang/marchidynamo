package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marchi/marchidynamo/ring"
	"github.com/marchi/marchidynamo/store"
)

// Config is process flags for one node.
type Config struct {
	ID      string
	Listen  string
	Token   uint64
	Peers   []string // id=token=host:port
	N, W, R int
	DataDir string
	HTTP    *http.Client
	// HintInterval is how often to retry hinted handoff (default 200ms).
	HintInterval time.Duration
}

// Node is a Dynamo-style replica that can coordinate writes.
type Node struct {
	cfg    Config
	store  *store.Store
	ring   *ring.Ring
	client *http.Client
	cancel context.CancelFunc
	hintMu sync.Mutex
}

// OpenStore starts the WAL-backed map, builds the static ring, and writes node.json.
func OpenStore(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("id required")
	}
	if cfg.N == 0 {
		cfg.N = 3
	}
	if cfg.W == 0 {
		cfg.W = 2
	}
	if cfg.R == 0 {
		cfg.R = 2
	}
	st, err := store.Open(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	peers, err := ring.ParseMembers(cfg.Peers)
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	self := ring.Member{ID: cfg.ID, Token: cfg.Token, Addr: cfg.Listen}
	members := append([]ring.Member{self}, peers...)
	rng, err := ring.New(members, cfg.N)
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	cli := cfg.HTTP
	if cli == nil {
		cli = &http.Client{Timeout: 2 * time.Second}
	}
	n := &Node{cfg: cfg, store: st, ring: rng, client: cli}
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, "hints"), 0o755); err != nil {
		_ = st.Close()
		return nil, err
	}
	if err := n.writeMeta(); err != nil {
		_ = st.Close()
		return nil, err
	}
	interval := cfg.HintInterval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	go n.hintLoop(ctx, interval)
	return n, nil
}

func (n *Node) writeMeta() error {
	type meta struct {
		ID      string        `json:"id"`
		Token   uint64        `json:"token"`
		Peers   []string      `json:"peers"`
		Members []ring.Member `json:"members"`
	}
	b, err := json.MarshalIndent(meta{
		ID:      n.cfg.ID,
		Token:   n.cfg.Token,
		Peers:   n.cfg.Peers,
		Members: n.ring.Members(),
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(n.cfg.DataDir, "node.json"), append(b, '\n'), 0o644)
}

// Close stops hint replay and releases the store.
func (n *Node) Close() error {
	if n.cancel != nil {
		n.cancel()
	}
	return n.store.Close()
}

// Ring is the static membership ring.
func (n *Node) Ring() *ring.Ring { return n.ring }

// Handler returns the HTTP mux.
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", n.handleHealthz)
	mux.HandleFunc("/kv/", n.handleKV)
	mux.HandleFunc("/internal/replicate", n.handleReplicate)
	mux.HandleFunc("/internal/read", n.handleRead)
	return mux
}

func (n *Node) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"id":      n.cfg.ID,
		"token":   n.cfg.Token,
		"listen":  n.cfg.Listen,
		"n":       n.cfg.N,
		"w":       n.cfg.W,
		"r":       n.cfg.R,
		"peers":   n.cfg.Peers,
		"members": n.ring.Members(),
	})
}

func (n *Node) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" || strings.Contains(key, "/") {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodPut:
		n.handlePut(w, r, key)
	case http.MethodGet:
		n.handleGet(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (n *Node) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ts := time.Now().UnixNano()
	if raw := r.Header.Get("X-Ts"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			http.Error(w, "invalid X-Ts", http.StatusBadRequest)
			return
		}
		ts = parsed
	}
	rec := store.Record{Key: key, Value: string(body), Ts: ts, Origin: n.cfg.ID}
	pref := n.ring.PreferenceList(key)
	type wr struct {
		id  string
		err error
	}
	ch := make(chan wr, len(pref))
	for _, m := range pref {
		go func(m ring.Member) {
			ch <- wr{m.ID, n.writeReplica(m, rec)}
		}(m)
	}
	acked := make([]string, 0, len(pref))
	for range pref {
		got := <-ch
		if got.err == nil {
			acked = append(acked, got.id)
		}
	}
	if len(acked) < n.cfg.W {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok":       false,
			"error":    "write quorum not met",
			"w":        n.cfg.W,
			"acked":    acked,
			"required": n.cfg.W,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"key":      key,
		"ts":       rec.Ts,
		"value":    rec.Value,
		"replicas": acked,
	})
}

func (n *Node) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	pref := n.ring.PreferenceList(key)
	ch := make(chan replicaView, len(pref))
	for _, m := range pref {
		go func(m ring.Member) {
			rec, ok, err := n.readReplica(m, key)
			ch <- replicaView{id: m.ID, rec: rec, ok: ok, err: err}
		}(m)
	}
	var best store.Record
	var found bool
	replicas := make([]string, 0, len(pref))
	answered := 0
	results := make([]replicaView, 0, len(pref))
	for range pref {
		got := <-ch
		results = append(results, got)
		if got.err != nil {
			continue
		}
		answered++
		if got.ok {
			replicas = append(replicas, got.id)
			if !found || store.Newer(got.rec, best) {
				best = got.rec
				found = true
			}
		}
	}
	if answered < n.cfg.R {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok":    false,
			"error": "read quorum not met",
			"r":     n.cfg.R,
			"got":   answered,
		})
		return
	}
	if !found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	n.readRepair(pref, results, best)
	writeJSON(w, http.StatusOK, map[string]any{
		"value":    best.Value,
		"ts":       best.Ts,
		"origin":   best.Origin,
		"replicas": replicas,
	})
}

func (n *Node) handleReplicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var rec store.Record
	if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if rec.Key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}
	if err := n.store.Put(rec); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "id": n.cfg.ID})
}

func (n *Node) handleRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}
	rec, ok := n.store.Get(key)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (n *Node) writeReplica(m ring.Member, rec store.Record) error {
	err := n.replicate(m, rec)
	if err != nil && m.ID != n.cfg.ID {
		_ = n.appendHint(m.ID, rec)
	}
	return err
}

func (n *Node) replicate(m ring.Member, rec store.Record) error {
	if m.ID == n.cfg.ID {
		return n.store.Put(rec)
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	url := "http://" + m.Addr + "/internal/replicate"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, b)
	}
	return nil
}

func (n *Node) readReplica(m ring.Member, key string) (store.Record, bool, error) {
	if m.ID == n.cfg.ID {
		rec, ok := n.store.Get(key)
		return rec, ok, nil
	}
	url := "http://" + m.Addr + "/internal/read?key=" + url.QueryEscape(key)
	resp, err := n.client.Get(url)
	if err != nil {
		return store.Record{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return store.Record{}, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return store.Record{}, false, fmt.Errorf("status %d: %s", resp.StatusCode, b)
	}
	var rec store.Record
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		return store.Record{}, false, err
	}
	return rec, true, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
