package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/marchi/marchidynamo/store"
)

// Config is process flags for one node.
type Config struct {
	ID      string
	Listen  string
	Token   uint64
	Peers   []string // host:port of other nodes
	N, W, R int
	DataDir string
}

// Node is a single Dynamo-style replica (slice 0: local store only).
type Node struct {
	cfg   Config
	store *store.Store
}

// OpenStore starts the WAL-backed map and writes node.json.
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
	n := &Node{cfg: cfg, store: st}
	if err := n.writeMeta(); err != nil {
		_ = st.Close()
		return nil, err
	}
	return n, nil
}

func (n *Node) writeMeta() error {
	type meta struct {
		ID    string   `json:"id"`
		Token uint64   `json:"token"`
		Peers []string `json:"peers"`
	}
	b, err := json.MarshalIndent(meta{ID: n.cfg.ID, Token: n.cfg.Token, Peers: n.cfg.Peers}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(n.cfg.DataDir, "node.json"), append(b, '\n'), 0o644)
}

// Close releases the store.
func (n *Node) Close() error {
	return n.store.Close()
}

// Handler returns the HTTP mux.
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", n.handleHealthz)
	mux.HandleFunc("/kv/", n.handleKV)
	return mux
}

func (n *Node) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"id":     n.cfg.ID,
		"token":  n.cfg.Token,
		"listen": n.cfg.Listen,
		"n":      n.cfg.N,
		"w":      n.cfg.W,
		"r":      n.cfg.R,
		"peers":  n.cfg.Peers,
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
	if err := n.store.Put(rec); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":    true,
		"key":   key,
		"ts":    rec.Ts,
		"value": rec.Value,
	})
}

func (n *Node) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	rec, ok := n.store.Get(key)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"value":    rec.Value,
		"ts":       rec.Ts,
		"origin":   rec.Origin,
		"replicas": []string{n.cfg.ID},
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
