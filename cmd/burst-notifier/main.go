package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/manlikeNacho/burst-notifier/internal/api"
	"github.com/manlikeNacho/burst-notifier/internal/debounce"
	"github.com/manlikeNacho/burst-notifier/internal/sender"
)

func main() {
	port := env("PORT", "8080")
	hookURL := os.Getenv("WEBHOOK_URL")

	// With no WEBHOOK_URL the service delivers to its own demo receiver, so a
	// single deployment shows the whole flow at GET /demo/received.
	var demo *api.DemoReceiver
	if hookURL == "" {
		demo = api.NewDemoReceiver()
		hookURL = "http://127.0.0.1:" + port + "/demo/receiver"
		log.Printf("WEBHOOK_URL not set; delivering to built-in demo receiver")
	}

	deb := debounce.New(sender.NewWebhook(hookURL, os.Getenv("WEBHOOK_SECRET")), debounce.Config{
		QuietWindow: envDur("QUIET_WINDOW", 5*time.Second),
		MaxWait:     envDur("MAX_WAIT", 30*time.Second),
		MaxSamples:  envInt("MAX_SAMPLES", 5),
		OnDead: func(b debounce.Batch, err error) {
			log.Printf("DEAD batch=%s count=%d err=%v", b.ID, b.Count, err)
		},
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go deb.Run(ctx, 500*time.Millisecond)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           api.New(deb, os.Getenv("API_KEY"), demo),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Fatalf("invalid duration for %s: %q", k, v)
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Fatalf("invalid int for %s: %q", k, v)
	}
	return def
}
