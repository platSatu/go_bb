package sender

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"go_bizbos/internal/notify"
	"go_bizbos/internal/store"
)

var (
	ErrCampaignNotFound = errors.New("Blast tidak ditemukan.")
	ErrCampaignState    = errors.New("Status blast tidak bisa diubah dari kondisi sekarang.")
)

// Enqueue menaruh pesan manual di antrean. client_ref sama = pesan yang sama (aman bila bizbos mengirim ulang).
func (s *Sender) Enqueue(ctx context.Context, item *store.Outbox) (*store.Outbox, error) {
	item.ID = store.NewID()
	item.Status = store.OutQueued
	item.Priority = store.PriorityManual
	item.AvailableAt = time.Now()
	if err := s.db.Clauses(clause.OnConflict{DoNothing: true}).Create(item).Error; err != nil {
		return nil, err
	}
	if item.ClientRef != nil {
		var existing store.Outbox
		if err := s.db.First(&existing, "branch_id = ? AND client_ref = ?", item.BranchID, *item.ClientRef).Error; err == nil {
			item = &existing
		}
	}
	s.Kick(ctx, item.DeviceID)
	return item, nil
}

// Campaign: start | pause | cancel. Baris penerima dibuat bizbos langsung di wa_outbox (priority blast).
func (s *Sender) Campaign(ctx context.Context, branch, id, action string) (*store.Campaign, error) {
	var c store.Campaign
	err := s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&c, "id = ? AND branch_id = ?", id, branch).Error; err != nil {
			return ErrCampaignNotFound
		}
		now := time.Now()
		switch {
		case action == "start" && (c.Status == store.CampaignDraft || c.Status == store.CampaignPaused):
			var total int64
			tx.Model(&store.Outbox{}).Where("campaign_id = ?", c.ID).Count(&total)
			c.Status, c.Total = store.CampaignRunning, int(total)
			if c.StartedAt == nil {
				c.StartedAt = &now
			}
		case action == "pause" && c.Status == store.CampaignRunning:
			c.Status = store.CampaignPaused
		case action == "cancel" && c.Status != store.CampaignDone && c.Status != store.CampaignCancelled:
			c.Status, c.FinishedAt = store.CampaignCancelled, &now
			tx.Model(&store.Outbox{}).Where("campaign_id = ? AND status = ?", c.ID, store.OutQueued).
				Updates(map[string]any{"status": store.OutCancelled, "error": "Blast dibatalkan."})
		default:
			return ErrCampaignState
		}
		return tx.Save(&c).Error
	})
	if err != nil {
		return nil, err
	}
	s.bus.Emit(c.BranchID, c.DeviceID, notify.CampaignStatus, map[string]any{"campaign_id": c.ID, "status": c.Status, "total": c.Total, "sent": c.Sent, "failed": c.Failed})
	if c.Status == store.CampaignRunning {
		s.Kick(ctx, c.DeviceID)
	}
	return &c, nil
}
