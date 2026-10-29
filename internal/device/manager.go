package device

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"gorm.io/gorm"

	"go_bizbos/internal/notify"
	dbs "go_bizbos/internal/store"

	_ "modernc.org/sqlite"
)

var (
	ErrNotFound      = errors.New("Perangkat tidak ditemukan.")
	ErrAlreadyLinked = errors.New("Perangkat ini sudah terhubung. Logout dulu bila ingin mengganti nomor.")
	ErrNotReady      = errors.New("Perangkat belum terhubung ke WhatsApp.")
	ErrBadPhone      = errors.New("Nomor HP tidak valid. Gunakan format 628xxxxxxxxxx.")
	phoneRe          = regexp.MustCompile(`^[1-9][0-9]{7,14}$`)
)

// Session: satu client whatsmeow untuk satu device. Event diproses berurutan per device.
type Session struct {
	DeviceID string
	BranchID string
	Client   *whatsmeow.Client

	events chan any
	done   chan struct{}
	stop   sync.Once

	mu sync.Mutex
	qr string
}

func (s *Session) QR() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.qr
}

func (s *Session) setQR(code string) {
	s.mu.Lock()
	s.qr = code
	s.mu.Unlock()
}

type Manager struct {
	db        *gorm.DB
	container *sqlstore.Container
	bus       *notify.Bus
	waLog     waLog.Logger

	mu       sync.Mutex
	sessions map[string]*Session
	locks    sync.Map // device_id -> *sync.Mutex, satu proses connect/logout per device

	// Diisi main: OnEvent untuk pesan/receipt, OnReady saat device siap kirim.
	OnEvent func(*Session, any)
	OnReady func(deviceID string)
}

func NewManager(ctx context.Context, db *gorm.DB, dataDir string, bus *notify.Bus) (*Manager, error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	store.SetOSInfo("Bizbos", [3]uint32{2, 0, 0})
	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_DESKTOP.Enum()

	dsn := fmt.Sprintf("file:%s?_pragma=foreign_keys(1)&_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)", filepath.Join(dataDir, "session.db"))
	container, err := sqlstore.New(ctx, "sqlite", dsn, waLog.Stdout("store", "WARN", false))
	if err != nil {
		return nil, fmt.Errorf("session store: %w", err)
	}
	return &Manager{db: db, container: container, bus: bus, waLog: waLog.Stdout("wa", "WARN", false), sessions: map[string]*Session{}}, nil
}

func (m *Manager) lock(id string) func() {
	v, _ := m.locks.LoadOrStore(id, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// Find selalu memeriksa branch: device milik branch lain dianggap tidak ada.
func (m *Manager) Find(branch, id string) (*dbs.Device, error) {
	var d dbs.Device
	if err := m.db.Where("id = ? AND branch_id = ?", id, branch).First(&d).Error; err != nil {
		return nil, ErrNotFound
	}
	return &d, nil
}

func (m *Manager) Session(id string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[id]
}

// Client mengembalikan client yang siap kirim, atau nil.
func (m *Manager) Client(id string) *whatsmeow.Client {
	if s := m.Session(id); s != nil && s.Client.IsConnected() && s.Client.IsLoggedIn() {
		return s.Client
	}
	return nil
}

// Restore menyambungkan ulang semua device yang pernah login.
func (m *Manager) Restore(ctx context.Context) {
	var rows []dbs.Device
	m.db.Where("jid IS NOT NULL AND status NOT IN ?", []string{dbs.DeviceLoggedOut, dbs.DeviceReplaced}).Find(&rows)

	sem := make(chan struct{}, 10) // jangan serbu server WhatsApp saat restart
	var wg sync.WaitGroup
	for i := range rows {
		d := &rows[i]
		sd := m.stored(ctx, d)
		if sd == nil {
			m.markLoggedOut(d.BranchID, d.ID, "Sesi tidak ditemukan, silakan hubungkan ulang.")
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			unlock := m.lock(d.ID)
			defer unlock()
			if m.Session(d.ID) != nil {
				return // sudah disambungkan lewat Connect
			}
			s := m.open(d, sd)
			if err := s.Client.Connect(); err != nil {
				m.setStatus(s, dbs.DeviceDisconnected, "Gagal menyambung, akan dicoba lagi.", nil)
			}
		}()
	}
	wg.Wait()
}

// Mode hasil Connect.
const (
	ModeReconnect = "reconnect" // sesi lama dipakai lagi, tanpa scan
	ModeQR        = "qr"        // QR dikirim lewat SSE device.qr
	ModeCode      = "code"      // kode pairing dikembalikan
	ModeConnected = "connected" // sudah tersambung ke WhatsApp, tidak perlu scan
)

// Connect memulai pairing. phone kosong = QR; diisi = kode pairing.
func (m *Manager) Connect(ctx context.Context, branch, id, phone string) (code, mode string, err error) {
	unlock := m.lock(id)
	defer unlock()

	d, err := m.Find(branch, id)
	if err != nil {
		return "", "", err
	}
	if phone != "" && !phoneRe.MatchString(phone) {
		return "", "", ErrBadPhone
	}
	if s := m.Session(id); s != nil {
		if s.Client.Store.ID != nil {
			if !s.Client.IsConnected() {
				return "", ModeReconnect, s.Client.Connect()
			}
			// Sudah tersambung: bukan kesalahan, cukup beri tahu bizbos supaya tampilannya disegarkan.
			// Untuk mengganti nomor tetap harus Disconnect dulu.
			return "", ModeConnected, nil
		}
		m.close(s) // pairing lama dibuang, mulai yang baru
	}

	// Sesi lama masih tersimpan (mis. setelah "dibuka di server lain"): cukup sambung ulang, tanpa scan.
	if sd := m.stored(ctx, d); sd != nil {
		if phone == "" {
			s := m.open(d, sd)
			m.setStatus(s, dbs.DeviceConnecting, "", nil)
			return "", ModeReconnect, s.Client.Connect()
		}
		_ = sd.Delete(ctx) // minta kode pairing = ganti nomor/sesi
	}

	s := m.open(d, m.container.NewDevice())
	qrCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	qrs, err := s.Client.GetQRChannel(qrCtx)
	if err != nil {
		cancel()
		m.close(s)
		return "", "", err
	}
	if err := s.Client.Connect(); err != nil {
		cancel()
		m.close(s)
		return "", "", fmt.Errorf("Gagal menyambung ke WhatsApp: %w", err)
	}

	first := make(chan struct{})
	go m.watchQR(s, qrs, first, cancel)
	m.setStatus(s, dbs.DeviceConnecting, "", nil)

	if phone == "" {
		return "", ModeQR, nil
	}
	select {
	case <-first:
	case <-time.After(20 * time.Second):
		return "", "", errors.New("WhatsApp belum siap, silakan coba lagi.")
	case <-ctx.Done():
		return "", "", ctx.Err()
	}
	code, err = s.Client.PairPhone(ctx, phone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
	if err != nil {
		return "", "", fmt.Errorf("Gagal membuat kode pairing: %w", err)
	}
	return code, ModeCode, nil
}

func (m *Manager) watchQR(s *Session, qrs <-chan whatsmeow.QRChannelItem, first chan struct{}, cancel context.CancelFunc) {
	defer cancel()
	var once sync.Once
	for item := range qrs {
		switch item.Event {
		case whatsmeow.QRChannelEventCode:
			s.setQR(item.Code)
			once.Do(func() { close(first) })
			m.setStatus(s, dbs.DevicePairing, "", nil)
			m.bus.Emit(s.BranchID, s.DeviceID, notify.DeviceQR, map[string]any{"qr": item.Code, "expires_in": int(item.Timeout.Seconds())})
		case whatsmeow.QRChannelSuccess.Event:
			s.setQR("")
		default:
			s.setQR("")
			if !s.Client.IsLoggedIn() {
				m.setStatus(s, dbs.DeviceDisconnected, "Waktu pairing habis. Silakan ulangi.", nil)
				m.close(s)
			}
		}
	}
}

func (m *Manager) Logout(ctx context.Context, branch, id string) error {
	unlock := m.lock(id)
	defer unlock()

	d, err := m.Find(branch, id)
	if err != nil {
		return err
	}
	if s := m.Session(id); s != nil {
		if s.Client.IsLoggedIn() {
			if err := s.Client.Logout(ctx); err != nil {
				s.Client.Disconnect()
				_ = s.Client.Store.Delete(ctx)
			}
		}
		m.close(s)
	} else if sd := m.stored(ctx, d); sd != nil {
		_ = sd.Delete(ctx)
	}
	m.markLoggedOut(branch, id, "Logout dari bizbos.")
	return nil
}

func (m *Manager) stored(ctx context.Context, d *dbs.Device) *store.Device {
	if d.JID == nil {
		return nil
	}
	jid, err := types.ParseJID(*d.JID)
	if err != nil {
		return nil
	}
	sd, err := m.container.GetDevice(ctx, jid)
	if err != nil {
		return nil
	}
	return sd
}

func (m *Manager) Shutdown() {
	m.mu.Lock()
	list := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		list = append(list, s)
	}
	m.mu.Unlock()
	for _, s := range list {
		s.Client.Disconnect()
	}
	_ = m.container.Close()
}

func (m *Manager) open(d *dbs.Device, sd *store.Device) *Session {
	client := whatsmeow.NewClient(sd, m.waLog)
	client.EnableAutoReconnect = true
	s := &Session{DeviceID: d.ID, BranchID: d.BranchID, Client: client, events: make(chan any, 512), done: make(chan struct{})}
	client.AddEventHandler(func(evt any) {
		select {
		case s.events <- evt:
		case <-s.done:
		}
	})
	go m.loop(s)

	m.mu.Lock()
	m.sessions[d.ID] = s
	m.mu.Unlock()
	return s
}

func (m *Manager) close(s *Session) {
	s.stop.Do(func() {
		close(s.done)
		s.Client.Disconnect()
		m.mu.Lock()
		if m.sessions[s.DeviceID] == s {
			delete(m.sessions, s.DeviceID)
		}
		m.mu.Unlock()
	})
}

func (m *Manager) loop(s *Session) {
	for {
		select {
		case <-s.done:
			return
		case evt := <-s.events:
			m.handle(s, evt)
		}
	}
}

func (m *Manager) setStatus(s *Session, status, reason string, extra map[string]any) {
	m.changeStatus(s.BranchID, s.DeviceID, status, reason, extra)
}

func (m *Manager) markLoggedOut(branch, id, reason string) {
	m.changeStatus(branch, id, dbs.DeviceLoggedOut, reason, map[string]any{"jid": nil})
}

// changeStatus mencatat riwayat hanya saat status benar-benar berubah (bahan skor kesehatan).
func (m *Manager) changeStatus(branch, id, status, reason string, extra map[string]any) {
	changed := m.db.Model(&dbs.Device{}).Where("id = ? AND status <> ?", id, status).
		Updates(map[string]any{"status": status, "status_reason": nullable(reason)}).RowsAffected > 0
	if len(extra) > 0 {
		m.db.Model(&dbs.Device{}).Where("id = ?", id).Updates(extra)
	}
	if changed {
		ev := dbs.DeviceEvent{DeviceID: id, Status: status}
		if reason != "" {
			ev.Reason = &reason
		}
		m.db.Create(&ev)
	}
	m.bus.Emit(branch, id, notify.DeviceStatus, map[string]any{"status": status, "reason": reason})
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func logf(s *Session, format string, args ...any) {
	log.Printf("[device %s] "+format, append([]any{s.DeviceID}, args...)...)
}
