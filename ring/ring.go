package ring

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
)

// Member is one static node on the token ring.
type Member struct {
	ID    string `json:"id"`
	Token uint64 `json:"token"`
	Addr  string `json:"addr"`
}

// Ring is a static consistent-hash ring (one token per node).
type Ring struct {
	members []Member // sorted by token
	n       int      // replication factor
}

// New copies members, sorts by token, and stores replication factor n.
func New(members []Member, n int) (*Ring, error) {
	if len(members) == 0 {
		return nil, fmt.Errorf("ring: no members")
	}
	if n <= 0 {
		n = 1
	}
	seen := make(map[string]struct{}, len(members))
	cp := make([]Member, len(members))
	copy(cp, members)
	for _, m := range cp {
		if m.ID == "" {
			return nil, fmt.Errorf("ring: member missing id")
		}
		if _, ok := seen[m.ID]; ok {
			return nil, fmt.Errorf("ring: duplicate id %s", m.ID)
		}
		seen[m.ID] = struct{}{}
	}
	sort.Slice(cp, func(i, j int) bool {
		if cp[i].Token == cp[j].Token {
			return cp[i].ID < cp[j].ID
		}
		return cp[i].Token < cp[j].Token
	})
	return &Ring{members: cp, n: n}, nil
}

// HashKey is FNV-1a 64 of the key (position on the ring).
func HashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

// Members returns ring members in token order.
func (r *Ring) Members() []Member {
	out := make([]Member, len(r.members))
	copy(out, r.members)
	return out
}

// N is the replication factor.
func (r *Ring) N() int { return r.n }

// PreferenceList walks clockwise from hash(key) and returns up to N distinct nodes.
func (r *Ring) PreferenceList(key string) []Member {
	return r.PreferenceListHash(HashKey(key))
}

// PreferenceListHash is PreferenceList for a precomputed token.
func (r *Ring) PreferenceListHash(h uint64) []Member {
	if len(r.members) == 0 {
		return nil
	}
	start := 0
	for i, m := range r.members {
		if m.Token >= h {
			start = i
			break
		}
	}
	// if every token < h, wrap to index 0 (start stays 0)
	want := r.n
	if want > len(r.members) {
		want = len(r.members)
	}
	out := make([]Member, 0, want)
	for i := 0; i < len(r.members) && len(out) < want; i++ {
		out = append(out, r.members[(start+i)%len(r.members)])
	}
	return out
}

// ParseMember parses "id=token=host:port".
func ParseMember(s string) (Member, error) {
	parts := strings.Split(s, "=")
	if len(parts) != 3 {
		return Member{}, fmt.Errorf("peer %q: want id=token=addr", s)
	}
	id := strings.TrimSpace(parts[0])
	addr := strings.TrimSpace(parts[2])
	if id == "" || addr == "" {
		return Member{}, fmt.Errorf("peer %q: empty id or addr", s)
	}
	tok, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil {
		return Member{}, fmt.Errorf("peer %q: token: %w", s, err)
	}
	return Member{ID: id, Token: tok, Addr: addr}, nil
}

// ParseMembers parses a comma-separated peer list.
func ParseMembers(specs []string) ([]Member, error) {
	out := make([]Member, 0, len(specs))
	for _, s := range specs {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		m, err := ParseMember(s)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}
