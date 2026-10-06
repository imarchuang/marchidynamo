package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHintedHandoffOrReadRepairCatchup(t *testing.T) {
	c := startCluster(t, 3, 2, 2)
	c.kill(2) // d3
	code, body := c.put(0, "cart", "n=2", "200")
	if code != 200 {
		t.Fatalf("put while d3 down: %d %s", code, body)
	}
	hint := filepath.Join(c.cfgs[0].DataDir, "hints", "d3.jsonl")
	if _, err := os.Stat(hint); err != nil {
		t.Fatalf("expected hint file on coordinator: %v", err)
	}

	c.restart(2)
	deadline := time.Now().Add(3 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		_, err := readInternal(c.addr[2], "cart")
		if err == nil {
			return
		}
		last = err
		// poke read repair as well as waiting for hint replay
		_, _ = c.get(0, "cart")
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("d3 did not catch up: %v", last)
}
