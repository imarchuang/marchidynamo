package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
}

// Node is a Dynamo-style replica that can coordinate writes.
type Node struct {
	cfg    Config
	store  *store.Store
	ring   *ring.Ring
	client *http.Client
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
	if err := n.writeMeta(); err != nil {
		_ = st.Close()
		return nil, err
	}
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

// Close releases the store.
func (n *Node) Close() error {
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
	acked := make([]string, 0, len(pref))
	for _, m := range pref {
		if err := n.writeReplica(m, rec); err != nil {
			http.Error(w, fmt.Sprintf("replica %s: %v", m.ID, err), http.StatusBadGateway)
			return
		}
		acked = append(acked, m.ID)
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
	var best store.Record
	var found bool
	replicas := make([]string, 0, len(pref))
	for _, m := range pref {
		rec, err := n.readReplica(m, key)
		if err != nil {
			continue
		}
		replicas = append(replicas, m.ID)
		if !found || rec.Ts > best.Ts {
			best = rec
			found = true
		}
	}
	if !found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
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

func (n *Node) readReplica(m ring.Member, key string) (store.Record, error) {
	if m.ID == n.cfg.ID {
		rec, ok := n.store.Get(key)
		if !ok {
			return store.Record{}, fmt.Errorf("not found")
		}
		return rec, nil
	}
	url := "http://" + m.Addr + "/internal/read?key=" + key
	resp, err := n.client.Get(url)
	if err != nil {
		return store.Record{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return store.Record{}, fmt.Errorf("status %d: %s", resp.StatusCode, b)
	}
	var rec store.Record
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		return store.Record{}, err
	}
	return rec, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
