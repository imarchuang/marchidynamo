package server

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

type testCluster struct {
	t     *testing.T
	nodes []*Node
	srvs  []*http.Server
	addr  []string
	ids   []string
	cfgs  []Config
}

func startCluster(t *testing.T, nFactor, w, r int) *testCluster {
	t.Helper()
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
	cfgs := make([]Config, 0, 3)
	cli := &http.Client{Timeout: 400 * time.Millisecond}
	for i := 0; i < 3; i++ {
		var peers []string
		for j := 0; j < 3; j++ {
			if i == j {
				continue
			}
			peers = append(peers, fmt.Sprintf("%s=%d=%s", ids[j], tokens[j], addr[j]))
		}
		cfg := Config{
			ID:           ids[i],
			Listen:       addr[i],
			Token:        tokens[i],
			Peers:        peers,
			N:            nFactor,
			W:            w,
			R:            r,
			DataDir:      t.TempDir(),
			HTTP:         cli,
			HintInterval: 50 * time.Millisecond,
		}
		cfgs = append(cfgs, cfg)
		node, err := OpenStore(cfg)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = node
		srvs[i] = &http.Server{Handler: node.Handler()}
		go func(s *http.Server, l net.Listener) {
			_ = s.Serve(l)
		}(srvs[i], listeners[i])
	}
	c := &testCluster{t: t, nodes: nodes, srvs: srvs, addr: addr, ids: ids, cfgs: cfgs}
	t.Cleanup(c.close)
	return c
}

func (c *testCluster) close() {
	for i := range c.srvs {
		if c.srvs[i] != nil {
			_ = c.srvs[i].Close()
		}
		if c.nodes[i] != nil {
			_ = c.nodes[i].Close()
		}
	}
}

func (c *testCluster) kill(i int) {
	c.t.Helper()
	if c.srvs[i] != nil {
		_ = c.srvs[i].Close()
	}
	if c.nodes[i] != nil {
		_ = c.nodes[i].Close()
	}
}

func (c *testCluster) restart(i int) {
	c.t.Helper()
	n, err := OpenStore(c.cfgs[i])
	if err != nil {
		c.t.Fatal(err)
	}
	ln, err := net.Listen("tcp", c.addr[i])
	if err != nil {
		c.t.Fatal(err)
	}
	srv := &http.Server{Handler: n.Handler()}
	go func() { _ = srv.Serve(ln) }()
	c.nodes[i] = n
	c.srvs[i] = srv
}

func (c *testCluster) put(coord int, key, val, ts string) (int, string) {
	c.t.Helper()
	req, err := http.NewRequest(http.MethodPut, "http://"+c.addr[coord]+"/kv/"+key, strings.NewReader(val))
	if err != nil {
		c.t.Fatal(err)
	}
	if ts != "" {
		req.Header.Set("X-Ts", ts)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func (c *testCluster) get(coord int, key string) (int, string) {
	c.t.Helper()
	resp, err := http.Get("http://" + c.addr[coord] + "/kv/" + key)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestQuorumW2SucceedsW3FailsWithOneDown(t *testing.T) {
	c2 := startCluster(t, 3, 2, 2)
	c2.kill(2) // d3
	code, body := c2.put(0, "cart", `{"n":2}`, "20")
	if code != 200 {
		t.Fatalf("W=2 put with d3 down: %d %s", code, body)
	}
	code, body = c2.get(0, "cart")
	if code != 200 {
		t.Fatalf("R=2 get with d3 down: %d %s", code, body)
	}
	if !strings.Contains(body, `"ts":20`) {
		t.Fatalf("get body %s", body)
	}

	c3 := startCluster(t, 3, 3, 2)
	c3.kill(2)
	code, body = c3.put(0, "cart", `{"n":3}`, "30")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("W=3 put with d3 down want 503 got %d %s", code, body)
	}
}
