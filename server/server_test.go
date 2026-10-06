package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthzPutGet(t *testing.T) {
	n, err := OpenStore(Config{
		ID:      "d1",
		Listen:  ":0",
		Token:   100,
		DataDir: t.TempDir(),
		N:       3, W: 2, R: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	ts := httptest.NewServer(n.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
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

	req, err := http.NewRequest(http.MethodPut, ts.URL+"/kv/cart", strings.NewReader(`{"n":1}`))
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

	resp, err = http.Get(ts.URL + "/kv/cart")
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

	resp, err = http.Get(ts.URL + "/kv/missing")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("missing want 404 got %d", resp.StatusCode)
	}
}
