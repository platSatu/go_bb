package device

import (
	"context"
	"fmt"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	dbs "go_bizbos/internal/store"
)

func (m *Manager) handle(s *Session, evt any) {
	switch e := evt.(type) {
	case *events.PairSuccess:
		s.setQR("")
		now := time.Now()
		m.db.Model(&dbs.Device{}).Where("id = ?", s.DeviceID).Updates(map[string]any{
			"jid": e.ID.String(), "phone": e.ID.User, "platform": e.Platform,
		})
		m.db.Model(&dbs.Device{}).Where("id = ? AND warmup_started_at IS NULL", s.DeviceID).Update("warmup_started_at", now)

	case *events.Connected:
		extra := map[string]any{"last_connected_at": time.Now()}
		if name := s.Client.Store.PushName; name != "" {
			extra["push_name"] = name
		}
		m.setStatus(s, dbs.DeviceConnected, "", extra)
		if s.Client.Store.PushName != "" {
			// Nama tampil ke penerima, tanpa menandai "online" supaya notifikasi di HP tetap jalan.
			go s.Client.SendPresence(context.Background(), types.PresenceUnavailable)
		}
		if m.OnReady != nil {
			m.OnReady(s.DeviceID)
		}

	case *events.Disconnected:
		m.setStatus(s, dbs.DeviceDisconnected, "Koneksi terputus, menyambung ulang otomatis.", nil)

	case *events.LoggedOut:
		_ = s.Client.Store.Delete(context.Background())
		m.close(s)
		m.markLoggedOut(s.BranchID, s.DeviceID, "WhatsApp di HP mengeluarkan perangkat ini. Silakan hubungkan ulang.")

	case *events.StreamReplaced:
		// Sesi yang sama dibuka di proses lain: mengalah, jangan saling rebut.
		m.close(s)
		m.setStatus(s, dbs.DeviceReplaced, "Sesi dibuka di server lain.", nil)

	case *events.TemporaryBan:
		until := time.Now().Add(e.Expire)
		m.setStatus(s, dbs.DeviceBanned, fmt.Sprintf("Nomor dibatasi sementara oleh WhatsApp (kode %d). Blast dihentikan otomatis.", e.Code), map[string]any{"banned_until": until})
		m.db.Model(&dbs.Campaign{}).Where("device_id = ? AND status = ?", s.DeviceID, dbs.CampaignRunning).Update("status", dbs.CampaignPaused)

	case *events.ConnectFailure:
		m.setStatus(s, dbs.DeviceDisconnected, fmt.Sprintf("WhatsApp menolak koneksi (kode %d).", e.Reason), nil)

	case *events.ClientOutdated:
		m.setStatus(s, dbs.DeviceDisconnected, "Versi WhatsApp server perlu diperbarui. Hubungi admin bizbos.", nil)

	default:
		if m.OnEvent != nil {
			m.OnEvent(s, evt)
		}
	}
}

// Watch menyambung ulang sesi yang sudah login tapi terputus (mis. gagal saat restart).
func (m *Manager) Watch(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		m.mu.Lock()
		var idle []*Session
		for _, s := range m.sessions {
			if s.Client.Store.ID != nil && !s.Client.IsConnected() {
				idle = append(idle, s)
			}
		}
		m.mu.Unlock()
		for _, s := range idle {
			if err := s.Client.Connect(); err != nil {
				logf(s, "reconnect gagal: %v", err)
			}
		}
	}
}
