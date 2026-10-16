package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"gorm.io/gorm"

	"go_bizbos/internal/auth"
	"go_bizbos/internal/store"
)

var backoff = []time.Duration{10 * time.Second, time.Minute, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour, 6 * time.Hour}

// Outbox: event disimpan dulu ke DB lalu dikirim ke bizbos, jadi tidak hilang saat bizbos sedang down.
type Outbox struct {
	db     *gorm.DB
	url    string
	secret string
	http   *http.Client
	wake   chan struct{}
}

func New(db *gorm.DB, url, secret string) *Outbox {
	return &Outbox{db: db, url: url, secret: secret, http: &http.Client{Timeout: 10 * time.Second}, wake: make(chan struct{}, 1)}
}

func (o *Outbox) Enabled() bool { return o.url != "" }

func (o *Outbox) Enqueue(branch, event string, data any) {
	if !o.Enabled() {
		return
	}
	payload, err := json.Marshal(map[string]any{"event": event, "branch_id": branch, "data": data, "created_at": time.Now().Unix()})
	if err != nil {
		return
	}
	row := store.WebhookEvent{BranchID: branch, Event: event, Payload: string(payload), Status: "pending", NextAttemptAt: time.Now()}
	if err := o.db.Create(&row).Error; err != nil {
		log.Printf("webhook: gagal simpan %s: %v", event, err)
		return
	}
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

func (o *Outbox) Run(ctx context.Context) {
	if !o.Enabled() {
		return
	}
	tick := time.NewTicker(3 * time.Second)
	prune := time.NewTicker(time.Hour)
	defer tick.Stop()
	defer prune.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-prune.C:
			o.db.Where("status = ? AND updated_at < ?", "sent", time.Now().AddDate(0, 0, -7)).Delete(&store.WebhookEvent{})
		case <-tick.C:
		case <-o.wake:
		}
		o.flush(ctx)
	}
}

func (o *Outbox) flush(ctx context.Context) {
	var rows []store.WebhookEvent
	o.db.Where("status = ? AND next_attempt_at <= ?", "pending", time.Now()).Order("id").Limit(100).Find(&rows)

	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for i := range rows {
		sem <- struct{}{}
		wg.Add(1)
		go func(r *store.WebhookEvent) {
			defer func() { <-sem; wg.Done() }()
			o.deliver(ctx, r)
		}(&rows[i])
	}
	wg.Wait()
}

func (o *Outbox) deliver(ctx context.Context, r *store.WebhookEvent) {
	err := o.post(ctx, r)
	r.Attempts++
	updates := map[string]any{"attempts": r.Attempts}
	switch {
	case err == nil:
		updates["status"], updates["last_error"] = "sent", nil
	case r.Attempts > len(backoff):
		updates["status"], updates["last_error"] = "failed", err.Error()
	default:
		updates["next_attempt_at"], updates["last_error"] = time.Now().Add(backoff[r.Attempts-1]), err.Error()
	}
	o.db.Model(&store.WebhookEvent{}).Where("id = ?", r.ID).Updates(updates)
}

func (o *Outbox) post(ctx context.Context, r *store.WebhookEvent) error {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.url, bytes.NewBufferString(r.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Bizbos-Event", r.Event)
	req.Header.Set("X-Bizbos-Delivery", strconv.FormatUint(r.ID, 10))
	req.Header.Set("X-Bizbos-Timestamp", ts)
	req.Header.Set("X-Bizbos-Signature", auth.Sign(o.secret, ts+"."+r.Payload))
	resp, err := o.http.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}
