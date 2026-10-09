package debounce

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type recorder struct {
	mu      sync.Mutex
	batches []Batch
	failN   int // fail this many calls first
	calls   int
}

func (r *recorder) Send(_ context.Context, b Batch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.failN > 0 {
		r.failN--
		return errors.New("receiver down")
	}
	r.batches = append(r.batches, b)
	return nil
}

func newTest(cfg Config) (*Debouncer, *fakeClock, *recorder) {
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	rec := &recorder{}
	cfg.Clock = clk
	return New(rec, cfg), clk, rec
}

func TestBurstBecomesOneBatch(t *testing.T) {
	d, clk, rec := newTest(Config{QuietWindow: 5 * time.Second, MaxWait: time.Minute})
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		if err := d.Add(Event{ID: fmt.Sprint(i), Key: "partner-1", Type: "order.created"}); err != nil {
			t.Fatal(err)
		}
		clk.Advance(20 * time.Millisecond) // 100 events over 2s
	}
	d.Flush(ctx)
	if len(rec.batches) != 0 {
		t.Fatal("flushed before the quiet window elapsed")
	}

	clk.Advance(5 * time.Second)
	d.Flush(ctx)

	if len(rec.batches) != 1 {
		t.Fatalf("want 1 batch, got %d", len(rec.batches))
	}
	b := rec.batches[0]
	if b.Count != 100 || b.Types["order.created"] != 100 || b.Reason != "quiet" {
		t.Fatalf("unexpected batch: %+v", b)
	}
	if len(b.Samples) != 5 {
		t.Fatalf("want 5 samples, got %d", len(b.Samples))
	}
}

func TestMaxWaitStopsStarvation(t *testing.T) {
	d, clk, rec := newTest(Config{QuietWindow: 5 * time.Second, MaxWait: 30 * time.Second})
	ctx := context.Background()

	// An event every 2s never lets the key go quiet.
	for i := 0; i < 20; i++ {
		_ = d.Add(Event{Key: "k", Type: "t"})
		clk.Advance(2 * time.Second)
		d.Flush(ctx)
	}
	if len(rec.batches) == 0 {
		t.Fatal("constant stream was never flushed")
	}
	if rec.batches[0].Reason != "max_wait" {
		t.Fatalf("want max_wait, got %q", rec.batches[0].Reason)
	}
}

func TestKeysDebounceIndependently(t *testing.T) {
	d, clk, rec := newTest(Config{QuietWindow: 5 * time.Second, MaxWait: time.Minute})
	ctx := context.Background()

	_ = d.Add(Event{Key: "a", Type: "t"})
	clk.Advance(4 * time.Second)
	_ = d.Add(Event{Key: "b", Type: "t"})
	clk.Advance(2 * time.Second) // a is 6s quiet, b is 2s quiet
	d.Flush(ctx)

	if len(rec.batches) != 1 || rec.batches[0].Key != "a" {
		t.Fatalf("want only a flushed, got %+v", rec.batches)
	}
	if d.Pending() != 1 {
		t.Fatalf("want b still pending, got %d", d.Pending())
	}
}

func TestDuplicateEventIDIsRejected(t *testing.T) {
	d, clk, rec := newTest(Config{QuietWindow: time.Second})
	ctx := context.Background()

	if err := d.Add(Event{ID: "evt-1", Key: "k", Type: "t"}); err != nil {
		t.Fatal(err)
	}
	if err := d.Add(Event{ID: "evt-1", Key: "k", Type: "t"}); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
	clk.Advance(2 * time.Second)
	d.Flush(ctx)
	if rec.batches[0].Count != 1 {
		t.Fatalf("duplicate was counted: %+v", rec.batches[0])
	}
}

func TestFailedSendRetriesWithBackoff(t *testing.T) {
	d, clk, rec := newTest(Config{QuietWindow: time.Second, RetryBase: 2 * time.Second, MaxAttempts: 5})
	rec.failN = 2
	ctx := context.Background()

	_ = d.Add(Event{Key: "k", Type: "t"})
	clk.Advance(time.Second)
	d.Flush(ctx) // attempt 1 fails, retry in 2s
	d.Flush(ctx) // too early: no new attempt
	if rec.calls != 1 {
		t.Fatalf("retried before backoff, calls=%d", rec.calls)
	}
	clk.Advance(2 * time.Second)
	d.Flush(ctx) // attempt 2 fails, retry in 4s
	clk.Advance(4 * time.Second)
	d.Flush(ctx) // attempt 3 succeeds

	if len(rec.batches) != 1 || rec.calls != 3 {
		t.Fatalf("batches=%d calls=%d", len(rec.batches), rec.calls)
	}
}

func TestExhaustedRetriesGoToDeadHandler(t *testing.T) {
	var dead []Batch
	d, clk, rec := newTest(Config{
		QuietWindow: time.Second, RetryBase: time.Second, MaxAttempts: 3,
		OnDead: func(b Batch, _ error) { dead = append(dead, b) },
	})
	rec.failN = 100
	ctx := context.Background()

	_ = d.Add(Event{Key: "k", Type: "t"})
	clk.Advance(time.Second)
	for i := 0; i < 10; i++ {
		d.Flush(ctx)
		clk.Advance(10 * time.Second)
	}
	if len(dead) != 1 || rec.calls != 3 {
		t.Fatalf("dead=%d calls=%d", len(dead), rec.calls)
	}
}

func TestBatchIDIsStableAcrossRetries(t *testing.T) {
	var ids []string
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	calls := 0
	s := senderFunc(func(b Batch) error {
		ids = append(ids, b.ID)
		calls++
		if calls < 2 {
			return errors.New("boom")
		}
		return nil
	})
	d := New(s, Config{QuietWindow: time.Second, RetryBase: time.Second, Clock: clk})
	_ = d.Add(Event{Key: "k", Type: "t"})
	clk.Advance(time.Second)
	d.Flush(context.Background())
	clk.Advance(time.Second)
	d.Flush(context.Background())
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("batch id changed across retries: %v", ids)
	}
}

func TestConcurrentAddIsRaceFree(t *testing.T) {
	d, clk, rec := newTest(Config{QuietWindow: time.Second})
	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = d.Add(Event{ID: fmt.Sprintf("%d-%d", g, i), Key: "k", Type: "t"})
			}
		}(g)
	}
	wg.Wait()
	clk.Advance(time.Second)
	d.Flush(context.Background())
	if len(rec.batches) != 1 || rec.batches[0].Count != 1000 {
		t.Fatalf("want one batch of 1000, got %+v", rec.batches)
	}
}

type senderFunc func(Batch) error

func (f senderFunc) Send(_ context.Context, b Batch) error { return f(b) }
