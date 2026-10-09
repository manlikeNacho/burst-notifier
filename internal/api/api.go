// Package api exposes the debouncer over HTTP.
package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"

	"github.com/manlikeNacho/burst-notifier/internal/debounce"
)

type Server struct {
	deb    *debounce.Debouncer
	apiKey string
	demo   *DemoReceiver
}

// New builds the HTTP handler. apiKey, when set, is required as a Bearer token
// on POST /events. demo, when set, mounts a built-in webhook receiver so a
// single deployed service can show the whole flow.
func New(deb *debounce.Debouncer, apiKey string, demo *DemoReceiver) http.Handler {
	s := &Server{deb: deb, apiKey: apiKey, demo: demo}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /events", s.events)
	if demo != nil {
		mux.HandleFunc("POST /demo/receiver", demo.receive)
		mux.HandleFunc("GET /demo/received", demo.list)
	}
	return mux
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "pending_keys": s.deb.Pending()})
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	if s.apiKey != "" {
		want := "Bearer " + s.apiKey
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte(want)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "body too large"})
		return
	}
	var ev debounce.Event
	if err := json.Unmarshal(body, &ev); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if ev.ID == "" {
		ev.ID = r.Header.Get("Idempotency-Key")
	}
	switch err := s.deb.Add(ev); {
	case errors.Is(err, debounce.ErrDuplicate):
		// Safe retry: the producer gets success without a double count.
		writeJSON(w, http.StatusOK, map[string]any{"accepted": false, "duplicate": true})
	case err != nil:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
	}
}

// DemoReceiver keeps the last few delivered batches in memory.
type DemoReceiver struct {
	mu   sync.Mutex
	last []debounce.Batch
	seen map[string]bool
}

func NewDemoReceiver() *DemoReceiver { return &DemoReceiver{seen: map[string]bool{}} }

func (d *DemoReceiver) receive(w http.ResponseWriter, r *http.Request) {
	var b debounce.Batch
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&b); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.seen[b.ID] { // dedupe redeliveries by batch id
		d.seen[b.ID] = true
		d.last = append([]debounce.Batch{b}, d.last...)
		if len(d.last) > 20 {
			d.last = d.last[:20]
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (d *DemoReceiver) list(w http.ResponseWriter, _ *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	writeJSON(w, http.StatusOK, d.last)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
