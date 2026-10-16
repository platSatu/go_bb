package api

import (
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"go_bizbos/internal/auth"
	"go_bizbos/internal/media"
)

const (
	heartbeat   = 25 * time.Second
	maxLifetime = time.Hour // browser menyambung ulang dengan token baru dari bizbos
)

// realtime: SSE per branch. Token: scope "sse", resource = device_id (opsional).
func (h *Handler) realtime(c *gin.Context) {
	tok, err := auth.ParseToken(h.Cfg.AccessSecret, c.Query("token"), "sse")
	if err != nil {
		fail(c, http.StatusUnauthorized, "Sesi realtime berakhir, silakan muat ulang halaman.")
		return
	}
	events, unsubscribe, err := h.Hub.Subscribe(tok.Branch, tok.Resource)
	if err != nil {
		fail(c, http.StatusTooManyRequests, "Terlalu banyak tab terbuka. Tutup sebagian tab lalu coba lagi.")
		return
	}
	defer unsubscribe()

	w := c.Writer
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "retry: 5000\n\n")
	w.Flush()

	ping := time.NewTicker(heartbeat)
	defer ping.Stop()
	end := time.NewTimer(maxLifetime)
	defer end.Stop()

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-end.C:
			fmt.Fprint(w, "event: reconnect\ndata: {}\n\n")
			w.Flush()
			return
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
		case e := <-events:
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Name, e.Data)
		}
		w.Flush()
	}
}

// media: token scope "media", resource = id pesan.
func (h *Handler) media(c *gin.Context) {
	tok, err := auth.ParseToken(h.Cfg.AccessSecret, c.Query("token"), "media")
	if err != nil || tok.Resource != c.Param("id") {
		fail(c, http.StatusUnauthorized, "Tautan file sudah kedaluwarsa, silakan muat ulang halaman.")
		return
	}
	f, err := h.Media.Get(c.Request.Context(), tok.Branch, tok.Resource)
	if err != nil {
		status := http.StatusBadGateway
		switch {
		case errors.Is(err, media.ErrNotFound):
			status = http.StatusNotFound
		case errors.Is(err, media.ErrGone):
			status = http.StatusGone
		case errors.Is(err, media.ErrRetrying):
			status = http.StatusAccepted
		case errors.Is(err, media.ErrNotReady):
			status = http.StatusServiceUnavailable
		}
		fail(c, status, err.Error())
		return
	}

	disposition := "attachment"
	if inlineSafe(f.Mime) {
		disposition = "inline"
	}
	name := f.Name
	if name == "" {
		name = "file"
	}
	c.Header("Content-Type", f.Mime)
	c.Header("Content-Disposition", mime.FormatMediaType(disposition, map[string]string{"filename": name}))
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("Content-Security-Policy", "default-src 'none'; sandbox")
	c.Header("Cache-Control", "private, max-age=3600")
	http.ServeFile(c.Writer, c.Request, f.Path)
}

// avatar: token scope "avatar", resource = id chat.
func (h *Handler) avatar(c *gin.Context) {
	tok, err := auth.ParseToken(h.Cfg.AccessSecret, c.Query("token"), "avatar")
	if err != nil || tok.Resource != c.Param("id") {
		c.Status(http.StatusUnauthorized)
		return
	}
	f, err := h.Media.Avatar(c.Request.Context(), tok.Branch, tok.Resource)
	if err != nil {
		c.Header("Cache-Control", "private, max-age=600")
		c.Status(http.StatusNotFound)
		return
	}
	c.Header("Content-Type", f.Mime)
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("Cache-Control", "private, max-age=3600")
	http.ServeFile(c.Writer, c.Request, f.Path)
}

// Hanya gambar/video/audio yang boleh tampil langsung; SVG/HTML/PDF selalu diunduh.
func inlineSafe(m string) bool {
	if m == "image/svg+xml" {
		return false
	}
	for _, p := range []string{"image/", "video/", "audio/"} {
		if strings.HasPrefix(m, p) {
			return true
		}
	}
	return false
}
