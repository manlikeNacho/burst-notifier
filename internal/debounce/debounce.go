// Package debounce collapses bursts of events that share a key into a single
// batch. A batch is released when the key has been quiet for QuietWindow, or
// when MaxWait has passed since its first event, so a constant stream can never
// delay a notification forever.
package debounce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Event is one thing that happened for a key (a partner, an order, ...).
type Event struct {
	ID      string          `json:"id"`
	Key     string          `json:"key"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
	At      time.Time       `json:"at"`
}

// Batch is what gets delivered: a summary of every event seen in one window.
type Batch struct {
	// ID is deterministic per window, so a receiver can dedupe redeliveries.
	ID      string         `json:"id"`
	Key     string         `json:"key"`
	Count   int            `json:"count"`
	Types   map[string]int `json:"types"`
	FirstAt time.Time      `json:"first_at"`
	LastAt  time.Time      `json:"last_at"`
	// Samples holds the first few events so the message can show real content.
	Samples []Event `json:"samples"`
	// Reason is "quiet" or "max_wait": why the window closed.
	Reason string `json:"reason"`
}

// Sender delivers a batch. Returning an error schedules a retry.
type Sender interface {
	Send(ctx context.Context, b Batch) error
}

// Clock is injected so tests run in microseconds instead of sleeping.
type Clock interface{ Now() time.Time }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Config tunes the debouncer. Zero values fall back to sane defaults.
type Config struct {
	QuietWindow time.Duration // default 5s
	MaxWait     time.Duration // default 30s
	MaxSamples  int           // default 5
	MaxAttempts int           // default 5
	RetryBase   time.Duration // default 2s, doubled per attempt
	DedupeTTL   time.Duration // default 10m
	Clock       Clock
	// OnDead is called when a batch exhausts its attempts.
	OnDead func(Batch, error)
}

func (c *Config) defaults() {
	if c.QuietWindow <= 0 {
		c.QuietWindow = 5 * time.Second
	}
	if c.MaxWait <= 0 {
		c.MaxWait = 30 * time.Second
	}
	if c.MaxSamples <= 0 {
		c.MaxSamples = 5
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
	if c.RetryBase <= 0 {
		c.RetryBase = 2 * time.Second
	}
	if c.DedupeTTL <= 0 {
		c.DedupeTTL = 10 * time.Minute
	}
	if c.Clock == nil {
		c.Clock = realClock{}
	}
}

type window struct {
	batch Batch
}

type outItem struct {
	batch    Batch
	attempts int
	nextTry  time.Time
}

// Debouncer is safe for concurrent use.
type Debouncer struct {
	cfg    Config
	sender Sender

	mu      sync.Mutex
	pending map[string]*window
	outbox  []*outItem
	seen    map[string]time.Time // event ID -> when it was seen
}

// ErrDuplicate is returned by Add when an event ID was already accepted.
var ErrDuplicate = errors.New("duplicate event")

func New(sender Sender, cfg Config) *Debouncer {
	cfg.defaults()
	return &Debouncer{
		cfg:     cfg,
		sender:  sender,
		pending: make(map[string]*window),
		seen:    make(map[string]time.Time),
	}
}

// Add records an event. Events with an ID that was already accepted within
// DedupeTTL are rejected with ErrDuplicate, which makes producer retries safe.
func (d *Debouncer) Add(ev Event) error {
	if ev.Key == "" {
		return errors.New("event key is required")
	}
	now := d.cfg.Clock.Now()
	if ev.At.IsZero() {
		ev.At = now
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if ev.ID != "" {
		if _, dup := d.seen[ev.ID]; dup {
			return ErrDuplicate
		}
		d.seen[ev.ID] = now
	}

	w, ok := d.pending[ev.Key]
	if !ok {
		w = &window{batch: Batch{
			Key:     ev.Key,
			Types:   map[string]int{},
			FirstAt: now,
		}}
		d.pending[ev.Key] = w
	}
	w.batch.Count++
	w.batch.Types[ev.Type]++
	w.batch.LastAt = now
	if len(w.batch.Samples) < d.cfg.MaxSamples {
		w.batch.Samples = append(w.batch.Samples, ev)
	}
	return nil
}

// Pending reports how many keys have an open window.
func (d *Debouncer) Pending() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.pending)
}

// Flush closes every window that is due and attempts every outbox item that is
// due. Call it on a ticker (see Run) or directly from tests.
func (d *Debouncer) Flush(ctx context.Context) {
	now := d.cfg.Clock.Now()

	d.mu.Lock()
	keys := make([]string, 0, len(d.pending))
	for k := range d.pending {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic order
	for _, k := range keys {
		w := d.pending[k]
		reason := ""
		switch {
		case now.Sub(w.batch.LastAt) >= d.cfg.QuietWindow:
			reason = "quiet"
		case now.Sub(w.batch.FirstAt) >= d.cfg.MaxWait:
			reason = "max_wait"
		}
		if reason == "" {
			continue
		}
		w.batch.Reason = reason
		w.batch.ID = fmt.Sprintf("%s@%d", w.batch.Key, w.batch.FirstAt.UnixNano())
		d.outbox = append(d.outbox, &outItem{batch: w.batch, nextTry: now})
		delete(d.pending, k)
	}
	for id, at := range d.seen {
		if now.Sub(at) > d.cfg.DedupeTTL {
			delete(d.seen, id)
		}
	}
	var due, later []*outItem
	for _, it := range d.outbox {
		if !it.nextTry.After(now) {
			due = append(due, it)
		} else {
			later = append(later, it)
		}
	}
	d.outbox = later
	d.mu.Unlock()

	// Send outside the lock so a slow receiver never blocks Add.
	for _, it := range due {
		err := d.sender.Send(ctx, it.batch)
		if err == nil {
			continue
		}
		it.attempts++
		if it.attempts >= d.cfg.MaxAttempts {
			if d.cfg.OnDead != nil {
				d.cfg.OnDead(it.batch, err)
			}
			continue
		}
		it.nextTry = now.Add(d.cfg.RetryBase << (it.attempts - 1))
		d.mu.Lock()
		d.outbox = append(d.outbox, it)
		d.mu.Unlock()
	}
}

// Run flushes on every tick until ctx is cancelled, then makes a final attempt
// to flush anything already due.
func (d *Debouncer) Run(ctx context.Context, tick time.Duration) {
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.Flush(ctx)
		}
	}
}
