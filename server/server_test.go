package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/marchi/marchidynamo/ring"
	"github.com/marchi/marchidynamo/store"
)

func TestCoordinatorForwardsAndStablePrefList(t *testing.T) {
	listeners := make([]net.Listener, 3)
	addr := make([]string, 3)
	for i := 0; i < 3; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = l
		addr[i] = l.Addr().String()
	}
	ids := []string{"d1", "d2", "d3"}
	tokens := []uint64{100, 200, 300}
	nodes := make([]*Node, 3)
	srvs := make([]*http.Server, 3)
	for i := 0; i < 3; i++ {
		var peers []string
		for j := 0; j < 3; j++ {
			if i == j {
				continue
			}
			peers = append(peers, fmt.Sprintf("%s=%d=%s", ids[j], tokens[j], addr[j]))
		}
		n, err := OpenStore(Config{
			ID:      ids[i],
			Listen:  addr[i],
			Token:   tokens[i],
			Peers:   peers,
			N:       2,
			W:       2,
			R:       2,
			DataDir: t.TempDir(),
			HTTP:    &http.Client{Timeout: 2 * time.Second},
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
		srvs[i] = &http.Server{Handler: n.Handler()}
		go func(s *http.Server, l net.Listener) {
			_ = s.Serve(l)
		}(srvs[i], listeners[i])
	}
	t.Cleanup(func() {
		for i := 0; i < 3; i++ {
			_ = srvs[i].Close()
			_ = nodes[i].Close()
		}
	})

	key := "cart"
	pref := nodes[0].Ring().PreferenceList(key)
	if len(pref) != 2 {
		t.Fatalf("pref %v", pref)
	}
	for i := 1; i < 3; i++ {
		got := nodes[i].Ring().PreferenceList(key)
		if !reflect.DeepEqual(memberIDs(pref), memberIDs(got)) {
			t.Fatalf("pref mismatch node %s: %v vs %v", ids[i], memberIDs(pref), memberIDs(got))
		}
	}
	for k := 0; k < 10; k++ {
		got := nodes[0].Ring().PreferenceList(key)
		if !reflect.DeepEqual(memberIDs(pref), memberIDs(got)) {
			t.Fatalf("unstable pref %v vs %v", memberIDs(pref), memberIDs(got))
		}
	}

	putURL := "http://" + addr[0] + "/kv/" + key
	req, err := http.NewRequest(http.MethodPut, putURL, strings.NewReader(`{"n":1}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Ts", "7")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("put %d %s", resp.StatusCode, body)
	}

	wantIDs := map[string]bool{}
	for _, m := range pref {
		wantIDs[m.ID] = true
		rec, err := readInternal(m.Addr, key)
		if err != nil {
			t.Fatalf("replica %s missing: %v", m.ID, err)
		}
		if rec.Value != `{"n":1}` || rec.Ts != 7 {
			t.Fatalf("replica %s %+v", m.ID, rec)
		}
	}
	for i, id := range ids {
		if wantIDs[id] {
			continue
		}
		_, err := readInternal(addr[i], key)
		if err == nil {
			t.Fatalf("non-replica %s should not store key", id)
		}
	}

	resp, err = http.Get("http://" + addr[0] + "/kv/" + key)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("get %d %s", resp.StatusCode, body)
	}
	var got struct {
		Value string `json:"value"`
		Ts    int64  `json:"ts"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Value != `{"n":1}` || got.Ts != 7 {
		t.Fatalf("coord get %+v", got)
	}
}

func memberIDs(ms []ring.Member) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}
	return out
}

func readInternal(addr, key string) (store.Record, error) {
	resp, err := http.Get("http://" + addr + "/internal/read?key=" + key)
	if err != nil {
		return store.Record{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return store.Record{}, fmt.Errorf("status %d %s", resp.StatusCode, b)
	}
	var rec store.Record
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		return store.Record{}, err
	}
	return rec, nil
}

func TestHealthzPutGet(t *testing.T) {
	n, err := OpenStore(Config{
		ID:      "d1",
		Listen:  "127.0.0.1:0",
		Token:   100,
		DataDir: t.TempDir(),
		N:       1, W: 1, R: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: n.Handler()}
	go srv.Serve(ln)
	defer srv.Close()
	base := "http://" + ln.Addr().String()

	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("healthz %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), `"id":"d1"`) {
		t.Fatalf("healthz body %s", body)
	}

	req, err := http.NewRequest(http.MethodPut, base+"/kv/cart", strings.NewReader(`{"n":1}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Ts", "99")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("put %d %s", resp.StatusCode, body)
	}

	resp, err = http.Get(base + "/kv/cart")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("get %d %s", resp.StatusCode, body)
	}
	var got struct {
		Value string `json:"value"`
		Ts    int64  `json:"ts"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Value != `{"n":1}` || got.Ts != 99 {
		t.Fatalf("get %+v body %s", got, body)
	}

	resp, err = http.Get(base + "/kv/missing")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("missing want 404 got %d", resp.StatusCode)
	}
}
