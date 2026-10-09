package sender

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/manlikeNacho/burst-notifier/internal/debounce"
)

func TestWebhookSignsAndDelivers(t *testing.T) {
	var gotSig, gotID string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSig = r.Header.Get("X-Signature")
		gotID = r.Header.Get("X-Batch-Id")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	b := debounce.Batch{ID: "k@1", Key: "k", Count: 3, FirstAt: time.Unix(0, 0), LastAt: time.Unix(1, 0)}
	if err := NewWebhook(srv.URL, "s3cret").Send(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if gotID != "k@1" {
		t.Fatalf("batch id header = %q", gotID)
	}
	if gotSig != "sha256="+Sign("s3cret", gotBody) {
		t.Fatalf("bad signature %q", gotSig)
	}
}

func TestWebhookNon2xxIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	if err := NewWebhook(srv.URL, "").Send(context.Background(), debounce.Batch{ID: "x"}); err == nil {
		t.Fatal("want error on 502")
	}
}
