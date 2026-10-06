package ring

import (
	"reflect"
	"testing"
)

func TestPreferenceListStable(t *testing.T) {
	r, err := New([]Member{
		{ID: "d1", Token: 100, Addr: "d1:8001"},
		{ID: "d2", Token: 200, Addr: "d2:8002"},
		{ID: "d3", Token: 300, Addr: "d3:8003"},
	}, 2)
	if err != nil {
		t.Fatal(err)
	}
	key := "cart"
	first := ids(r.PreferenceList(key))
	if len(first) != 2 {
		t.Fatalf("want N=2 got %v", first)
	}
	for i := 0; i < 20; i++ {
		got := ids(r.PreferenceList(key))
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("unstable: %v then %v", first, got)
		}
	}
	// clockwise from hash: token >= h, wrap
	pref100 := ids(r.PreferenceListHash(100))
	if !reflect.DeepEqual(pref100, []string{"d1", "d2"}) {
		t.Fatalf("hash 100: %v", pref100)
	}
	pref201 := ids(r.PreferenceListHash(201))
	if !reflect.DeepEqual(pref201, []string{"d3", "d1"}) {
		t.Fatalf("hash 201: %v", pref201)
	}
	pref301 := ids(r.PreferenceListHash(301))
	if !reflect.DeepEqual(pref301, []string{"d1", "d2"}) {
		t.Fatalf("wrap: %v", pref301)
	}
}

func TestNEqualsCluster(t *testing.T) {
	r, err := New([]Member{
		{ID: "d1", Token: 100},
		{ID: "d2", Token: 200},
		{ID: "d3", Token: 300},
	}, 3)
	if err != nil {
		t.Fatal(err)
	}
	got := ids(r.PreferenceList("any-key"))
	if len(got) != 3 {
		t.Fatalf("N=3 want 3 nodes got %v", got)
	}
	seen := map[string]bool{}
	for _, id := range got {
		seen[id] = true
	}
	if len(seen) != 3 {
		t.Fatalf("duplicate in pref list %v", got)
	}
}

func TestParseMember(t *testing.T) {
	m, err := ParseMember("d2=200=d2:8002")
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != "d2" || m.Token != 200 || m.Addr != "d2:8002" {
		t.Fatalf("%+v", m)
	}
}

func ids(ms []Member) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}
	return out
}
