package media

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

	"go_bizbos/internal/store"
)

const (
	avatarTTL      = 24 * time.Hour
	avatarMaxBytes = 5 << 20
	avatarGap      = 300 * time.Millisecond // jeda antar permintaan foto profil per device
)

var avatarHTTP = &http.Client{Timeout: 15 * time.Second}

type limiter struct {
	mu   sync.Mutex
	last time.Time
}

// Avatar: foto profil chat, disimpan di disk 24 jam (termasuk catatan "tidak ada foto").
func (s *Service) Avatar(ctx context.Context, branch, chatID string) (*File, error) {
	var chat store.Chat
	if s.db.First(&chat, "id = ? AND branch_id = ?", chatID, branch).Error != nil {
		return nil, ErrNotFound
	}
	base := filepath.Join(s.dir, "avatars", branch, chat.ID)
	file := &File{Path: base + ".jpg", Mime: "image/jpeg", Name: "avatar.jpg"}
	if fresh(base + ".none") {
		return nil, ErrNotFound
	}
	if fresh(file.Path) {
		return file, nil
	}

	cli := s.client(chat.DeviceID)
	if cli == nil {
		if _, err := os.Stat(file.Path); err == nil {
			return file, nil
		}
		return nil, ErrNotReady
	}
	jid, err := types.ParseJID(chat.ChatJID)
	if err != nil {
		return nil, ErrNotFound
	}

	v, _ := s.avatarLimit.LoadOrStore(chat.DeviceID, &limiter{})
	l := v.(*limiter)
	l.mu.Lock()
	if wait := avatarGap - time.Since(l.last); wait > 0 {
		time.Sleep(wait)
	}
	info, err := cli.GetProfilePictureInfo(ctx, jid, &whatsmeow.GetProfilePictureParams{Preview: true})
	l.last = time.Now()
	l.mu.Unlock()

	if errors.Is(err, whatsmeow.ErrProfilePictureNotSet) || errors.Is(err, whatsmeow.ErrProfilePictureUnauthorized) || (err == nil && info == nil) {
		_ = os.MkdirAll(filepath.Dir(base), 0o700)
		_ = os.WriteFile(base+".none", nil, 0o600)
		return nil, ErrNotFound
	}
	if err != nil || !trustedAvatarURL(info.URL) {
		return nil, ErrNotFound
	}
	if err := download(ctx, info.URL, file.Path); err != nil {
		return nil, ErrNotFound
	}
	_ = os.Remove(base + ".none")
	return file, nil
}

func fresh(path string) bool {
	st, err := os.Stat(path)
	return err == nil && time.Since(st.ModTime()) < avatarTTL
}

func trustedAvatarURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && (u.Hostname() == "whatsapp.net" || strings.HasSuffix(u.Hostname(), ".whatsapp.net"))
}

func download(ctx context.Context, src, dst string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, nil)
	if err != nil {
		return err
	}
	resp, err := avatarHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("status")
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".av-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	n, err := io.Copy(tmp, io.LimitReader(resp.Body, avatarMaxBytes+1))
	tmp.Close()
	if err != nil || n > avatarMaxBytes || n == 0 {
		return errors.New("size")
	}
	return os.Rename(tmp.Name(), dst)
}
