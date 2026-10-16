package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	window      = 5 * time.Minute
	memoryLimit = 1 << 20
	MaxBody     = 101 << 20
)

var uuidRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func IsUUID(s string) bool { return uuidRe.MatchString(s) }

func Sign(secret string, parts ...string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(m.Sum(nil))
}

// API memverifikasi request dari bizbos.
//
// Signature = hex(HMAC-SHA256(secret, ts \n nonce \n METHOD \n uri \n branch_id \n sha256(body)))
// Header: X-Bizbos-Timestamp, X-Bizbos-Nonce, X-Bizbos-Branch, X-Bizbos-Signature.
// Body dibaca dulu (memori atau file sementara) supaya handler tidak pernah memproses data yang belum terverifikasi.
func API(secret string) gin.HandlerFunc {
	nonces := newNonceCache()
	return func(c *gin.Context) {
		h := c.Request.Header
		ts, nonce, branch, sig := h.Get("X-Bizbos-Timestamp"), h.Get("X-Bizbos-Nonce"), h.Get("X-Bizbos-Branch"), h.Get("X-Bizbos-Signature")

		unix, err := strconv.ParseInt(ts, 10, 64)
		if err != nil || absDur(time.Since(time.Unix(unix, 0))) > window || len(nonce) < 16 || len(nonce) > 64 || !IsUUID(branch) || sig == "" {
			deny(c, "Permintaan tidak sah.")
			return
		}

		body, sum, cleanup, err := spool(c.Request)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{"success": false, "message": "Ukuran data terlalu besar."})
			return
		}

		want := Sign(secret, ts, nonce, c.Request.Method, c.Request.URL.RequestURI(), branch, sum)
		if !hmac.Equal([]byte(want), []byte(strings.ToLower(sig))) {
			cleanup()
			deny(c, "Tanda tangan tidak cocok.")
			return
		}
		if !nonces.add(nonce) {
			cleanup()
			deny(c, "Permintaan ini sudah pernah dipakai.")
			return
		}

		c.Request.Body = body
		c.Set("branch_id", branch)
		c.Next()
		cleanup()
	}
}

func Branch(c *gin.Context) string { return c.GetString("branch_id") }

func deny(c *gin.Context, msg string) {
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"success": false, "message": msg})
}

// spool membaca body (maks MaxBody) sambil menghitung sha256-nya.
func spool(r *http.Request) (io.ReadCloser, string, func(), error) {
	noop := func() {}
	hasher := sha256.New()
	src := io.TeeReader(io.LimitReader(r.Body, MaxBody+1), hasher)

	if r.ContentLength >= 0 && r.ContentLength <= memoryLimit {
		data, err := io.ReadAll(src)
		if err != nil || int64(len(data)) > memoryLimit {
			return nil, "", noop, errors.New("body")
		}
		return io.NopCloser(bytes.NewReader(data)), hex.EncodeToString(hasher.Sum(nil)), noop, nil
	}

	f, err := os.CreateTemp("", "bizbos-body-*")
	if err != nil {
		return nil, "", noop, err
	}
	cleanup := func() { f.Close(); os.Remove(f.Name()) }
	n, err := io.Copy(f, src)
	if err != nil || n > MaxBody {
		cleanup()
		return nil, "", noop, errors.New("body")
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, "", noop, err
	}
	return io.NopCloser(f), hex.EncodeToString(hasher.Sum(nil)), cleanup, nil
}

type nonceCache struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

func newNonceCache() *nonceCache { return &nonceCache{seen: map[string]time.Time{}} }

func (n *nonceCache) add(nonce string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	now := time.Now()
	if len(n.seen) > 10000 {
		for k, t := range n.seen {
			if now.Sub(t) > 2*window {
				delete(n.seen, k)
			}
		}
	}
	if _, ok := n.seen[nonce]; ok {
		return false
	}
	n.seen[nonce] = now
	return true
}

// Token singkat untuk browser, dibuat bizbos: base64url(json) "." hex(HMAC(access_secret, base64url(json))).
type Token struct {
	Branch   string `json:"b"`
	Scope    string `json:"s"`
	Resource string `json:"r,omitempty"`
	Exp      int64  `json:"exp"`
}

const maxTokenTTL = 15 * time.Minute

func ParseToken(secret, raw, scope string) (*Token, error) {
	payload, sig, ok := strings.Cut(raw, ".")
	if !ok || !hmac.Equal([]byte(Sign(secret, payload)), []byte(sig)) {
		return nil, errors.New("token tidak sah")
	}
	data, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return nil, errors.New("token tidak sah")
	}
	var t Token
	if json.Unmarshal(data, &t) != nil || t.Scope != scope || !IsUUID(t.Branch) {
		return nil, errors.New("token tidak sah")
	}
	left := time.Until(time.Unix(t.Exp, 0))
	if left <= 0 || left > maxTokenTTL {
		return nil, errors.New("token kedaluwarsa")
	}
	return &t, nil
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
