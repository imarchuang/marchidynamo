package server

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/marchi/marchidynamo/ring"
	"github.com/marchi/marchidynamo/store"
)

type replicaView struct {
	id  string
	rec store.Record
	ok  bool
	err error
}

func (n *Node) hintDir() string {
	return filepath.Join(n.cfg.DataDir, "hints")
}

func (n *Node) appendHint(nodeID string, rec store.Record) error {
	n.hintMu.Lock()
	defer n.hintMu.Unlock()
	if err := os.MkdirAll(n.hintDir(), 0o755); err != nil {
		return err
	}
	path := filepath.Join(n.hintDir(), nodeID+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func (n *Node) hintLoop(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n.replayHints()
		}
	}
}

func (n *Node) replayHints() {
	n.hintMu.Lock()
	defer n.hintMu.Unlock()
	entries, err := os.ReadDir(n.hintDir())
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".jsonl" {
			continue
		}
		id := e.Name()[:len(e.Name())-len(".jsonl")]
		m, ok := n.member(id)
		if !ok {
			continue
		}
		path := filepath.Join(n.hintDir(), e.Name())
		remaining := n.flushHintFile(path, m)
		if len(remaining) == 0 {
			_ = os.Remove(path)
			continue
		}
		_ = os.WriteFile(path, remaining, 0o644)
	}
}

func (n *Node) flushHintFile(path string, m ring.Member) []byte {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var leftover []byte
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := append([]byte(nil), sc.Bytes()...)
		if len(line) == 0 {
			continue
		}
		var rec store.Record
		if err := json.Unmarshal(line, &rec); err != nil {
			leftover = append(leftover, line...)
			leftover = append(leftover, '\n')
			continue
		}
		if err := n.replicate(m, rec); err != nil {
			leftover = append(leftover, line...)
			leftover = append(leftover, '\n')
		}
	}
	return leftover
}

func (n *Node) member(id string) (ring.Member, bool) {
	for _, m := range n.ring.Members() {
		if m.ID == id {
			return m, true
		}
	}
	return ring.Member{}, false
}

func (n *Node) readRepair(pref []ring.Member, views []replicaView, best store.Record) {
	byID := make(map[string]replicaView, len(views))
	for _, v := range views {
		byID[v.id] = v
	}
	for _, m := range pref {
		v, ok := byID[m.ID]
		if !ok || v.err != nil {
			continue
		}
		if !v.ok || store.Newer(best, v.rec) {
			go func(m ring.Member) {
				_ = n.replicate(m, best)
			}(m)
		}
	}
}
