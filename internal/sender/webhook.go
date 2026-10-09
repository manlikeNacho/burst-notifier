// Package sender delivers debounced batches to an HTTP webhook.
package sender

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/manlikeNacho/burst-notifier/internal/debounce"
)

// Webhook POSTs each batch as JSON. When Secret is set, the body is signed with
// HMAC-SHA256 in the X-Signature header so the receiver can verify the origin.
// X-Batch-Id is stable across retries so receivers can dedupe.
type Webhook struct {
	URL    string
	Secret string
	Client *http.Client
}

func NewWebhook(url, secret string) *Webhook {
	return &Webhook{URL: url, Secret: secret, Client: &http.Client{Timeout: 10 * time.Second}}
}

func (w *Webhook) Send(ctx context.Context, b debounce.Batch) error {
	body, err := json.Marshal(b)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Batch-Id", b.ID)
	if w.Secret != "" {
		req.Header.Set("X-Signature", "sha256="+Sign(w.Secret, body))
	}
	resp, err := w.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("webhook returned %d", resp.StatusCode)
	}
	return nil
}

// Sign returns the hex HMAC-SHA256 of body.
func Sign(secret string, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}
