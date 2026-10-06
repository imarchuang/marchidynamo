package server

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestLWWOlderHTTPPutDoesNotOverwrite(t *testing.T) {
	c := startCluster(t, 3, 2, 2)
	code, body := c.put(0, "cart", "winner", "100")
	if code != 200 {
		t.Fatalf("first put %d %s", code, body)
	}
	code, body = c.put(1, "cart", "stale", "50")
	if code != 200 {
		t.Fatalf("stale put should ack quorum, got %d %s", code, body)
	}
	code, body = c.get(2, "cart")
	if code != 200 {
		t.Fatalf("get %d %s", code, body)
	}
	var got struct {
		Value string `json:"value"`
		Ts    int64  `json:"ts"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	if got.Value != "winner" || got.Ts != 100 {
		t.Fatalf("lww %+v body %s", got, body)
	}
	if strings.Contains(body, "stale") {
		t.Fatalf("stale leaked %s", body)
	}
}
