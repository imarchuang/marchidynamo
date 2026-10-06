package server

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

func TestRingDebug(t *testing.T) {
	c := startCluster(t, 3, 2, 2)
	resp, err := http.Get("http://" + c.addr[0] + "/ring?key=cart")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("ring %d %s", resp.StatusCode, body)
	}
	var got struct {
		SampleKey string `json:"sample_key"`
		N         int    `json:"n"`
		Pref      []struct {
			ID string `json:"id"`
		} `json:"preference_list"`
		Members []struct {
			ID    string `json:"id"`
			Token uint64 `json:"token"`
		} `json:"members"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.SampleKey != "cart" || got.N != 3 {
		t.Fatalf("%+v body %s", got, body)
	}
	if len(got.Members) != 3 || len(got.Pref) != 3 {
		t.Fatalf("ring members/pref %s", body)
	}
	want := c.nodes[0].Ring().PreferenceList("cart")
	if got.Pref[0].ID != want[0].ID {
		t.Fatalf("pref[0]=%s want %s", got.Pref[0].ID, want[0].ID)
	}
}
