package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/manlikeNacho/burst-notifier/internal/debounce"
)

type nopSender struct{}

func (nopSender) Send(_ context.Context, _ debounce.Batch) error { return nil }

func newHandler(key string) (http.Handler, *debounce.Debouncer) {
	d := debounce.New(nopSender{}, debounce.Config{})
	return New(d, key, nil), d
}

func post(h http.Handler, body, auth string, hdr map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	if auth != "" {
		r.Header.Set("Authorization", auth)
	}
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestEventsRequireKeyWhenConfigured(t *testing.T) {
	h, _ := newHandler("secret")
	if w := post(h, `{"key":"k","type":"t"}`, "", nil); w.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", w.Code)
	}
	if w := post(h, `{"key":"k","type":"t"}`, "Bearer secret", nil); w.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d", w.Code)
	}
}

func TestIdempotencyKeyHeaderDedupes(t *testing.T) {
	h, d := newHandler("")
	hdr := map[string]string{"Idempotency-Key": "abc"}
	if w := post(h, `{"key":"k","type":"t"}`, "", hdr); w.Code != http.StatusAccepted {
		t.Fatalf("first: %d", w.Code)
	}
	w := post(h, `{"key":"k","type":"t"}`, "", hdr)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"duplicate":true`) {
		t.Fatalf("retry should be a no-op success, got %d %s", w.Code, w.Body.String())
	}
	if d.Pending() != 1 {
		t.Fatalf("pending = %d", d.Pending())
	}
}

func TestBadInput(t *testing.T) {
	h, _ := newHandler("")
	if w := post(h, `not json`, "", nil); w.Code != http.StatusBadRequest {
		t.Fatalf("bad json: %d", w.Code)
	}
	if w := post(h, `{"type":"t"}`, "", nil); w.Code != http.StatusBadRequest {
		t.Fatalf("missing key: %d", w.Code)
	}
}
