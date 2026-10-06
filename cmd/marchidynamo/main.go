package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/marchi/marchidynamo/server"
)

func main() {
	id := flag.String("id", envOr("MARCHIDYNAMO_ID", "d1"), "node id")
	listen := flag.String("listen", envOr("MARCHIDYNAMO_LISTEN", ":8001"), "listen address")
	token := flag.Uint64("token", envUint64("MARCHIDYNAMO_TOKEN", 100), "ring token")
	peers := flag.String("peers", envOr("MARCHIDYNAMO_PEERS", ""), "comma-separated peers id=token=host:port")
	n := flag.Int("n", envInt("MARCHIDYNAMO_N", 3), "replication factor")
	w := flag.Int("w", envInt("MARCHIDYNAMO_W", 2), "write quorum")
	r := flag.Int("r", envInt("MARCHIDYNAMO_R", 2), "read quorum")
	dataDir := flag.String("dataDir", envOr("MARCHIDYNAMO_DATA", "./data"), "per-node data directory")
	flag.Parse()

	node, err := server.OpenStore(server.Config{
		ID:      *id,
		Listen:  *listen,
		Token:   *token,
		Peers:   splitCSV(*peers),
		N:       *n,
		W:       *w,
		R:       *r,
		DataDir: *dataDir,
	})
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer node.Close()

	srv := &http.Server{Addr: *listen, Handler: node.Handler()}
	go func() {
		log.Printf("marchidynamo id=%s token=%d listen=%s n=%d w=%d r=%d", *id, *token, *listen, *n, *w, *r)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	_ = srv.Close()
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

func envUint64(k string, def uint64) uint64 {
	if v := os.Getenv(k); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err == nil {
			return n
		}
	}
	return def
}
