package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Record is one KV version as stored in the WAL and memory map.
type Record struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	Ts     int64  `json:"ts"`
	Origin string `json:"origin"`
}

// Store is an in-memory map with a JSONL write-ahead log.
// After Open, the memory map is the source of truth.
type Store struct {
	mu      sync.RWMutex
	data    map[string]Record
	wal     *os.File
	dataDir string
}

// Open creates dataDir if needed, replays wal.jsonl, and appends further writes.
func Open(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir dataDir: %w", err)
	}
	walPath := filepath.Join(dataDir, "wal.jsonl")
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	s := &Store{
		data:    make(map[string]Record),
		wal:     f,
		dataDir: dataDir,
	}
	if err := s.replay(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, os.SEEK_END); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("seek wal: %w", err)
	}
	return s, nil
}

func (s *Store) replay(f *os.File) error {
	if _, err := f.Seek(0, os.SEEK_SET); err != nil {
		return fmt.Errorf("rewind wal: %w", err)
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("wal line %d: %w", lineNo, err)
		}
		if rec.Key == "" {
			continue
		}
		s.data[rec.Key] = rec
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("scan wal: %w", err)
	}
	return nil
}

// Put appends rec to the WAL and overwrites the in-memory value.
func (s *Store) Put(rec Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putLocked(rec)
}

func (s *Store) putLocked(rec Record) error {
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := s.wal.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("wal write: %w", err)
	}
	if err := s.wal.Sync(); err != nil {
		return fmt.Errorf("wal sync: %w", err)
	}
	s.data[rec.Key] = rec
	return nil
}

// Get returns the current record for key.
func (s *Store) Get(key string) (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.data[key]
	return rec, ok
}

// Close flushes and closes the WAL.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal == nil {
		return nil
	}
	err := s.wal.Close()
	s.wal = nil
	return err
}

// DataDir is the on-disk root for this node.
func (s *Store) DataDir() string {
	return s.dataDir
}
