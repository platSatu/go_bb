package media

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"mime/multipart"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gabriel-vasile/mimetype"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"gorm.io/gorm"

	"go_bizbos/internal/notify"
	"go_bizbos/internal/store"
)

var (
	ErrNotFound = errors.New("File tidak ditemukan.")
	ErrNotReady = errors.New("Perangkat sedang tidak terhubung, file belum bisa diunduh.")
	ErrRetrying = errors.New("File sudah kedaluwarsa di server WhatsApp. Kami sedang memintanya ulang ke HP pengirim, coba lagi sebentar.")
	ErrGone     = errors.New("File ini sudah tidak tersedia lagi di WhatsApp.")
	ErrTooLarge = errors.New("Ukuran file melebihi batas.")
)

// Batas ukuran mengikuti WhatsApp.
var limits = map[string]int64{"image": 16 << 20, "video": 64 << 20, "audio": 16 << 20, "sticker": 1 << 20, "document": 100 << 20}

var waTypes = map[string]whatsmeow.MediaType{"image": whatsmeow.MediaImage, "sticker": whatsmeow.MediaImage, "video": whatsmeow.MediaVideo, "audio": whatsmeow.MediaAudio, "document": whatsmeow.MediaDocument}

// Meta: data minimal untuk mengunduh media nanti, disimpan di wa_messages.media_meta.
type Meta struct {
	Kind       string `json:"k"`
	DirectPath string `json:"p"`
	MediaKey   []byte `json:"mk"`
	SHA256     []byte `json:"s"`
	EncSHA256  []byte `json:"e"`
	Chat       string `json:"c"`
	Sender     string `json:"f"`
	IsGroup    bool   `json:"g,omitempty"`
	Gone       bool   `json:"x,omitempty"`
}

func (m *Meta) JSON() *string {
	b, _ := json.Marshal(m)
	s := string(b)
	return &s
}

type Service struct {
	db     *gorm.DB
	dir    string
	bus    *notify.Bus
	client func(deviceID string) *whatsmeow.Client
	locks  [64]sync.Mutex // satu unduhan per pesan pada satu waktu

	avatarLimit sync.Map // device_id -> *limiter

	prefetch chan prefetchJob
}

type prefetchJob struct{ branch, messageID string }

// Unduhan di belakang layar: file media pesan baru langsung disimpan (seperti WhatsApp di HP),
// jadi gambar/video tampil instan di inbox & panel Media dan tidak hilang walau server WhatsApp
// sudah menghapusnya. File besar tetap diunduh saat dibuka.
const (
	prefetchWorkers = 3
	prefetchQueue   = 2000
	prefetchMaxSize = 25 << 20
)

func New(db *gorm.DB, dataDir string, bus *notify.Bus, client func(string) *whatsmeow.Client) *Service {
	s := &Service{db: db, dir: dataDir, bus: bus, client: client, prefetch: make(chan prefetchJob, prefetchQueue)}
	for i := 0; i < prefetchWorkers; i++ {
		go s.prefetchWorker()
	}
	return s
}

// Prefetch mengantre unduhan media satu pesan. Antrean penuh = dilewati (tetap bisa diunduh saat dibuka).
func (s *Service) Prefetch(branch, messageID string) {
	select {
	case s.prefetch <- prefetchJob{branch, messageID}:
	default:
	}
}

func (s *Service) prefetchWorker() {
	for job := range s.prefetch {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[media] prefetch %s panic: %v", job.messageID, r)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			_, _ = s.get(ctx, job.branch, job.messageID, false)
		}()
	}
}

type File struct {
	Path, Mime, Name string
}

// Get mengembalikan file media pesan; diunduh dari WhatsApp saat pertama kali diminta.
func (s *Service) Get(ctx context.Context, branch, messageID string) (*File, error) {
	return s.get(ctx, branch, messageID, true)
}

// get: allowRetry=false untuk unduhan di belakang layar (tidak meminta HP mengunggah ulang file lama).
func (s *Service) get(ctx context.Context, branch, messageID string, allowRetry bool) (*File, error) {
	mu := &s.locks[fnv32(messageID)%uint32(len(s.locks))]
	mu.Lock()
	defer mu.Unlock()

	var msg store.Message
	if err := s.db.Where("id = ? AND branch_id = ? AND (media_meta IS NOT NULL OR media_path IS NOT NULL)", messageID, branch).First(&msg).Error; err != nil {
		return nil, ErrNotFound
	}
	if msg.MediaPath != nil {
		full := filepath.Join(s.dir, *msg.MediaPath)
		if _, err := os.Stat(full); err == nil {
			return &File{Path: full, Mime: deref(msg.MediaMime), Name: deref(msg.MediaName)}, nil
		}
	}

	var meta Meta
	if msg.MediaMeta == nil || json.Unmarshal([]byte(*msg.MediaMeta), &meta) != nil {
		return nil, ErrNotFound
	}
	if meta.Gone {
		return nil, ErrGone
	}
	if msg.MediaSize != nil && *msg.MediaSize > limits["document"] {
		return nil, ErrTooLarge
	}
	if !allowRetry && msg.MediaSize != nil && *msg.MediaSize > prefetchMaxSize {
		return nil, ErrTooLarge
	}
	cli := s.client(msg.DeviceID)
	if cli == nil {
		return nil, ErrNotReady
	}

	dctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	dir := filepath.Join(s.dir, "media", branch)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(dir, ".dl-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name())
	err = cli.DownloadMediaWithPathToFile(dctx, meta.DirectPath, meta.EncSHA256, meta.SHA256, meta.MediaKey, waTypes[meta.Kind], "", false, tmp)
	tmp.Close()
	if err != nil {
		if allowRetry && (errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) || errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) || errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410)) {
			return nil, s.requestRetry(ctx, cli, &msg, &meta)
		}
		return nil, errors.New("Gagal mengunduh file dari WhatsApp, silakan coba lagi.")
	}

	mt, err := mimetype.DetectFile(tmp.Name())
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(tmp.Name())
	if err != nil {
		return nil, err
	}
	rel := filepath.Join("media", branch, store.NewID()+mt.Extension())
	if err := os.Rename(tmp.Name(), filepath.Join(s.dir, rel)); err != nil {
		return nil, err
	}
	s.db.Model(&store.Message{}).Where("id = ?", msg.ID).Updates(map[string]any{"media_path": rel, "media_mime": mt.String(), "media_size": info.Size()})
	return &File{Path: filepath.Join(s.dir, rel), Mime: mt.String(), Name: deref(msg.MediaName)}, nil
}

func (s *Service) requestRetry(ctx context.Context, cli *whatsmeow.Client, msg *store.Message, meta *Meta) error {
	chat, err1 := types.ParseJID(meta.Chat)
	sender, err2 := types.ParseJID(meta.Sender)
	if err1 != nil || err2 != nil {
		return ErrGone
	}
	info := &types.MessageInfo{ID: msg.WaID, MessageSource: types.MessageSource{Chat: chat, Sender: sender, IsFromMe: msg.FromMe, IsGroup: meta.IsGroup}}
	if err := cli.SendMediaRetryReceipt(ctx, info, meta.MediaKey); err != nil {
		return errors.New("Gagal meminta ulang file, silakan coba lagi.")
	}
	return ErrRetrying
}

// HandleRetry: HP pengirim mengunggah ulang file, simpan direct path yang baru.
func (s *Service) HandleRetry(branch, deviceID string, evt *events.MediaRetry) {
	var msg store.Message
	if s.db.Where("device_id = ? AND wa_id = ? AND media_meta IS NOT NULL", deviceID, evt.MessageID).First(&msg).Error != nil {
		return
	}
	var meta Meta
	if json.Unmarshal([]byte(*msg.MediaMeta), &meta) != nil {
		return
	}
	notif, err := whatsmeow.DecryptMediaRetryNotification(evt, meta.MediaKey)
	ready := err == nil && notif.GetResult() == waMmsRetry.MediaRetryNotification_SUCCESS && notif.GetDirectPath() != ""
	if ready {
		meta.DirectPath = notif.GetDirectPath()
	} else {
		meta.Gone = true
	}
	s.db.Model(&store.Message{}).Where("id = ?", msg.ID).Update("media_meta", meta.JSON())
	s.bus.Emit(branch, deviceID, notify.MessageMedia, map[string]any{"message_id": msg.ID, "ready": ready})
}

// SaveUpload menyimpan file kiriman bizbos. Tipe file ditentukan dari isi file, bukan dari nama/header.
func (s *Service) SaveUpload(branch string, fh *multipart.FileHeader) (*store.Upload, string, error) {
	if fh.Size > limits["document"] {
		return nil, "", ErrTooLarge
	}
	f, err := fh.Open()
	if err != nil {
		return nil, "", err
	}
	defer f.Close()

	mt, err := mimetype.DetectReader(f)
	if err != nil {
		return nil, "", err
	}
	kind := KindOf(mt.String())
	if fh.Size > limits[kind] {
		kind = "document"
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, "", err
	}

	rel := filepath.Join("uploads", branch, store.NewID()+mt.Extension())
	full := filepath.Join(s.dir, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		return nil, "", err
	}
	out, err := os.OpenFile(full, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, "", err
	}
	n, err := io.Copy(out, io.LimitReader(f, limits["document"]+1))
	out.Close()
	if err != nil || n > limits["document"] {
		os.Remove(full)
		return nil, "", ErrTooLarge
	}

	up := &store.Upload{ID: store.NewID(), BranchID: branch, Path: rel, Mime: mt.String(), Name: cleanName(fh.Filename), Size: n}
	if err := s.db.Create(up).Error; err != nil {
		os.Remove(full)
		return nil, "", err
	}
	return up, kind, nil
}

func (s *Service) Open(up *store.Upload) (*os.File, error) {
	return os.Open(filepath.Join(s.dir, up.Path))
}

// KindOf: hanya format yang pasti diterima WhatsApp yang dikirim sebagai media, sisanya dokumen.
func KindOf(mime string) string {
	switch mime {
	case "image/jpeg", "image/png":
		return "image"
	case "video/mp4", "video/3gpp":
		return "video"
	case "audio/ogg", "audio/mpeg", "audio/mp4", "audio/aac", "audio/amr":
		return "audio"
	}
	return "document"
}

func WaType(kind string) whatsmeow.MediaType { return waTypes[kind] }

func fnv32(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h = (h ^ uint32(s[i])) * 16777619
	}
	return h
}

func cleanName(name string) string {
	name = filepath.Base(strings.ReplaceAll(name, "\\", "/"))
	name = strings.Map(func(r rune) rune {
		if r < 32 || strings.ContainsRune(`"<>|:*?`, r) {
			return -1
		}
		return r
	}, name)
	if r := []rune(name); len(r) > 150 {
		name = string(r[len(r)-150:])
	}
	if name == "" || name == "." {
		name = "file"
	}
	return name
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
