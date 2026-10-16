package sender

import (
	"context"
	"errors"
	"log"
	"math/rand/v2"
	"regexp"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"gorm.io/gorm"

	"go_bizbos/internal/config"
	"go_bizbos/internal/media"
	"go_bizbos/internal/message"
	"go_bizbos/internal/notify"
	"go_bizbos/internal/store"
)

const maxAttempts = 3

var digitsRe = regexp.MustCompile(`^[1-9][0-9]{7,14}$`)

// Sender: satu antrean per device (manual selalu didahulukan dari blast), dengan jeda acak,
// batas harian + pemanasan nomor baru, jam tenang, dan berhenti otomatis saat dibatasi WhatsApp.
type Sender struct {
	db     *gorm.DB
	cfg    *config.Config
	bus    *notify.Bus
	inbox  *message.Inbox
	media  *media.Service
	client func(string) *whatsmeow.Client

	slots   chan struct{} // batas kirim paralel seluruh server
	mu      sync.Mutex
	workers map[string]chan struct{}
}

func New(db *gorm.DB, cfg *config.Config, bus *notify.Bus, inbox *message.Inbox, med *media.Service, client func(string) *whatsmeow.Client) *Sender {
	return &Sender{db: db, cfg: cfg, bus: bus, inbox: inbox, media: med, client: client, slots: make(chan struct{}, cfg.MaxParallel), workers: map[string]chan struct{}{}}
}

// Kick membangunkan (atau menyalakan) worker sebuah device.
func (s *Sender) Kick(ctx context.Context, deviceID string) {
	s.mu.Lock()
	wake, ok := s.workers[deviceID]
	if !ok {
		wake = make(chan struct{}, 1)
		s.workers[deviceID] = wake
		go s.work(ctx, deviceID, wake)
	}
	s.mu.Unlock()
	select {
	case wake <- struct{}{}:
	default:
	}
}

// Run: bersihkan antrean yang terputus saat restart, lalu pantau baris yang diisi langsung oleh bizbos.
func (s *Sender) Run(ctx context.Context) {
	s.db.Model(&store.Outbox{}).Where("status = ?", store.OutSending).
		Updates(map[string]any{"status": store.OutFailed, "error": "Pengiriman terputus karena server dimulai ulang. Silakan kirim ulang bila belum diterima."})

	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		var ids []string
		s.db.Model(&store.Outbox{}).Distinct("device_id").Where("status = ? AND available_at <= ?", store.OutQueued, time.Now()).Pluck("device_id", &ids)
		for _, id := range ids {
			s.Kick(ctx, id)
		}
		uploads.Range(func(k, v any) bool {
			if time.Since(v.(cachedUpload).at) > time.Hour {
				uploads.Delete(k)
			}
			return true
		})
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Sender) work(ctx context.Context, deviceID string, wake chan struct{}) {
	var blastAfter time.Time
	idle := time.NewTimer(0)
	defer idle.Stop()

	for {
		wait := s.step(ctx, deviceID, &blastAfter)
		if wait > 0 {
			idle.Reset(wait)
			select {
			case <-ctx.Done():
				return
			case <-wake:
			case <-idle.C:
			}
		} else if ctx.Err() != nil {
			return
		}
	}
}

// step memproses satu item; mengembalikan lama menunggu (0 = langsung lanjut).
func (s *Sender) step(ctx context.Context, deviceID string, blastAfter *time.Time) time.Duration {
	cli := s.client(deviceID)
	if cli == nil {
		return time.Minute
	}
	var dev store.Device
	if s.db.First(&dev, "id = ?", deviceID).Error != nil {
		return 5 * time.Minute
	}

	now := time.Now()
	allowBlast, blastWait := s.blastAllowed(&dev, now, *blastAfter)
	item, err := s.next(deviceID, allowBlast, now)
	if err != nil {
		if allowBlast || blastWait == 0 {
			return 30 * time.Second
		}
		return min(blastWait, 30*time.Second)
	}
	if !s.claim(item) {
		return 0
	}

	s.slots <- struct{}{}
	err = s.deliver(ctx, cli, &dev, item)
	<-s.slots

	s.finish(item, err)
	if item.Priority >= store.PriorityBlast {
		jitter := s.cfg.BlastMinDelay + rand.N(s.cfg.BlastMaxDelay-s.cfg.BlastMinDelay+time.Second)
		*blastAfter = time.Now().Add(jitter)
		if err == nil {
			s.db.Exec("UPDATE wa_devices SET sent_today = IF(sent_date = ?, sent_today + 1, 1), sent_date = ? WHERE id = ?", s.today(), s.today(), deviceID)
		}
	}
	return 0
}

func (s *Sender) blastAllowed(dev *store.Device, now, after time.Time) (bool, time.Duration) {
	if dev.BannedUntil != nil && dev.BannedUntil.After(now) {
		return false, time.Until(*dev.BannedUntil)
	}
	if dev.Status == store.DeviceBanned {
		return false, time.Hour
	}
	if h := now.In(s.cfg.Location).Hour(); s.quiet(h) {
		return false, 15 * time.Minute
	}
	if sent := s.sentToday(dev); sent >= s.dailyCap(dev, now) {
		return false, time.Hour
	}
	if now.Before(after) {
		return false, after.Sub(now)
	}
	return true, 0
}

func (s *Sender) quiet(h int) bool {
	a, b := s.cfg.QuietStartHour, s.cfg.QuietEndHour
	if a == b {
		return false
	}
	if a > b {
		return h >= a || h < b
	}
	return h >= a && h < b
}

// dailyCap: nomor baru dipanaskan bertahap (20, 40, 60, ...) sampai batas harian.
func (s *Sender) dailyCap(dev *store.Device, now time.Time) int {
	limit := s.cfg.BlastDailyCap
	if dev.DailyCap > 0 {
		limit = dev.DailyCap
	}
	if dev.WarmupStartedAt != nil {
		days := int(now.Sub(*dev.WarmupStartedAt).Hours() / 24)
		limit = min(limit, 20*(days+1))
	}
	return limit
}

func (s *Sender) sentToday(dev *store.Device) int {
	if dev.SentDate != nil && len(*dev.SentDate) >= 10 && (*dev.SentDate)[:10] == s.today() {
		return dev.SentToday
	}
	return 0
}

func (s *Sender) today() string { return time.Now().In(s.cfg.Location).Format("2006-01-02") }

func (s *Sender) next(deviceID string, allowBlast bool, now time.Time) (*store.Outbox, error) {
	q := s.db.Where("device_id = ? AND status = ? AND available_at <= ?", deviceID, store.OutQueued, now).
		Where("campaign_id IS NULL OR EXISTS (SELECT 1 FROM wa_campaigns c WHERE c.id = wa_outbox.campaign_id AND c.status = ?)", store.CampaignRunning)
	if !allowBlast {
		q = q.Where("priority < ?", store.PriorityBlast)
	}
	var item store.Outbox
	err := q.Order("priority, available_at, created_at").First(&item).Error
	return &item, err
}

func (s *Sender) claim(item *store.Outbox) bool {
	res := s.db.Model(&store.Outbox{}).Where("id = ? AND status = ?", item.ID, store.OutQueued).
		Updates(map[string]any{"status": store.OutSending, "attempts": gorm.Expr("attempts + 1")})
	item.Attempts++
	return res.RowsAffected == 1
}

var (
	errOptOut    = errors.New("Penerima sudah berhenti berlangganan pesan blast.")
	errNotOnWA   = errors.New("Nomor ini tidak terdaftar di WhatsApp.")
	errBadTarget = errors.New("Nomor tujuan tidak valid.")
	errNoUpload  = errors.New("File lampiran tidak ditemukan.")
	errBadPoll   = errors.New("Data polling tidak valid.")
)

// skipErr: item dilewati tanpa dicoba ulang (nomor tidak valid, opt-out).
type skipErr struct{ error }

func (s *Sender) deliver(ctx context.Context, cli *whatsmeow.Client, dev *store.Device, item *store.Outbox) error {
	blast := item.Priority >= store.PriorityBlast
	jid, err := s.resolve(ctx, cli, item.ChatJID)
	if err != nil {
		return skipErr{err}
	}
	if blast && jid.Server == types.DefaultUserServer {
		var n int64
		s.db.Table("wa_optouts").Where("branch_id = ? AND phone = ?", item.BranchID, jid.User).Count(&n)
		if n > 0 {
			return skipErr{errOptOut}
		}
	}

	built, err := s.build(ctx, cli, item)
	if err != nil {
		return err
	}

	if blast {
		_ = cli.SendChatPresence(ctx, jid, types.ChatPresenceComposing, types.ChatPresenceMediaText)
		time.Sleep(time.Second + rand.N(2*time.Second))
		_ = cli.SendChatPresence(ctx, jid, types.ChatPresencePaused, types.ChatPresenceMediaText)
	}

	sctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	resp, err := cli.SendMessage(sctx, jid, built.msg)
	if err != nil {
		return err
	}

	msg := store.Message{
		ID: store.NewID(), BranchID: item.BranchID, DeviceID: item.DeviceID, WaID: resp.ID, FromMe: true,
		Type: built.kind, Body: item.Body, QuotedWaID: item.QuotedWaID, Status: store.MsgSent, SentAt: resp.Timestamp,
	}
	if cli.Store.ID != nil {
		msg.SenderJID = store.Ptr(cli.Store.ID.ToNonAD().String())
	}
	if built.upload != nil {
		msg.MediaPath, msg.MediaMime, msg.MediaName, msg.MediaSize = &built.upload.Path, &built.upload.Mime, &built.upload.Name, &built.upload.Size
	}
	if len(built.poll) > 0 {
		msg.Meta = message.PollMeta(built.poll)
	}
	ref := ""
	if item.ClientRef != nil {
		ref = *item.ClientRef
	}
	if s.inbox.SaveOutgoing(&msg, jid, previewOf(built.kind, item), ref) {
		item.MessageID = &msg.ID
	}
	return nil
}

func (s *Sender) resolve(ctx context.Context, cli *whatsmeow.Client, target string) (types.JID, error) {
	if digitsRe.MatchString(target) {
		res, err := cli.IsOnWhatsApp(ctx, []string{"+" + target})
		if err != nil {
			return types.JID{}, errBadTarget
		}
		if len(res) == 0 || !res[0].IsIn {
			return types.JID{}, errNotOnWA
		}
		return res[0].JID.ToNonAD(), nil
	}
	jid, err := types.ParseJID(target)
	if err != nil || (jid.Server != types.DefaultUserServer && jid.Server != types.GroupServer && jid.Server != types.HiddenUserServer) {
		return types.JID{}, errBadTarget
	}
	return jid.ToNonAD(), nil
}

func (s *Sender) finish(item *store.Outbox, err error) {
	updates := map[string]any{"error": nil}
	var skip skipErr
	switch {
	case err == nil:
		updates["status"], updates["message_id"] = store.OutSent, item.MessageID
	case errors.As(err, &skip):
		updates["status"], updates["error"] = store.OutSkipped, skip.Error()
	case errors.Is(err, errNoUpload) || errors.Is(err, errBadPoll):
		updates["status"], updates["error"] = store.OutFailed, err.Error()
	case item.Attempts < maxAttempts:
		updates["status"], updates["error"] = store.OutQueued, "Gagal, akan dicoba lagi otomatis."
		updates["available_at"] = time.Now().Add(time.Duration(item.Attempts) * 30 * time.Second)
		log.Printf("[outbox %s] percobaan %d gagal: %v", item.ID, item.Attempts, err)
	default:
		updates["status"], updates["error"] = store.OutFailed, "Pesan gagal dikirim. Pastikan WhatsApp di HP aktif lalu coba lagi."
	}
	s.db.Model(&store.Outbox{}).Where("id = ?", item.ID).Updates(updates)
	s.bus.Emit(item.BranchID, item.DeviceID, notify.OutboxStatus, map[string]any{"outbox_id": item.ID, "status": updates["status"], "error": updates["error"], "message_id": item.MessageID, "campaign_id": item.CampaignID})

	if item.CampaignID != nil && updates["status"] != store.OutQueued {
		s.campaignProgress(item, updates["status"] == store.OutSent)
	}
}

func (s *Sender) campaignProgress(item *store.Outbox, ok bool) {
	col := "failed"
	if ok {
		col = "sent"
	}
	id := *item.CampaignID
	s.db.Exec("UPDATE wa_campaigns SET "+col+" = "+col+" + 1, updated_at = ? WHERE id = ?", time.Now(), id)
	res := s.db.Exec(`UPDATE wa_campaigns SET status = ?, finished_at = ? WHERE id = ? AND status = ?
		AND NOT EXISTS (SELECT 1 FROM wa_outbox WHERE campaign_id = ? AND status IN ?)`,
		store.CampaignDone, time.Now(), id, store.CampaignRunning, id, []string{store.OutQueued, store.OutSending})
	if res.RowsAffected > 0 {
		var c store.Campaign
		if s.db.First(&c, "id = ?", id).Error == nil {
			s.bus.Emit(c.BranchID, c.DeviceID, notify.CampaignStatus, map[string]any{"campaign_id": c.ID, "status": c.Status, "total": c.Total, "sent": c.Sent, "failed": c.Failed})
		}
	}
}

func previewOf(kind string, item *store.Outbox) string {
	if item.Body != nil && *item.Body != "" && kind != "poll" {
		return *item.Body
	}
	return map[string]string{"image": "📷 Foto", "video": "🎥 Video", "audio": "🎤 Audio", "document": "📄 Dokumen", "poll": "📊 Polling"}[kind]
}
