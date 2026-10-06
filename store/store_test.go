package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPutGetReplay(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	rec := Record{Key: "cart", Value: `{"n":1}`, Ts: 42, Origin: "d1"}
	if err := s.Put(rec); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("cart")
	if !ok {
		t.Fatal("missing key after put")
	}
	if got != rec {
		t.Fatalf("got %+v want %+v", got, rec)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, ok = s2.Get("cart")
	if !ok {
		t.Fatal("missing key after wal replay")
	}
	if got != rec {
		t.Fatalf("replay %+v want %+v", got, rec)
	}
	if _, err := os.Stat(filepath.Join(dir, "wal.jsonl")); err != nil {
		t.Fatal(err)
	}
}

func TestGetMissing(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, ok := s.Get("nope"); ok {
		t.Fatal("expected miss")
	}
}
