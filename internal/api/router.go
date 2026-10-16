package api

import (
	"net/http"
	"slices"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"go_bizbos/internal/auth"
	"go_bizbos/internal/config"
	"go_bizbos/internal/device"
	"go_bizbos/internal/media"
	"go_bizbos/internal/realtime"
	"go_bizbos/internal/sender"
)

type Handler struct {
	DB      *gorm.DB
	Cfg     *config.Config
	Devices *device.Manager
	Media   *media.Service
	Sender  *sender.Sender
	Hub     *realtime.Hub
}

func (h *Handler) Router() *gin.Engine {
	if !h.Cfg.IsLocal() {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	r.Use(gin.Recovery())
	_ = r.SetTrustedProxies([]string{"127.0.0.1", "::1"})
	r.MaxMultipartMemory = 8 << 20

	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) })

	// Dipanggil server bizbos (HMAC).
	v1 := r.Group("/v1", auth.API(h.Cfg.APISecret))
	v1.POST("/devices/:id/connect", h.connect)
	v1.GET("/devices/:id/status", h.status)
	v1.POST("/devices/:id/logout", h.logout)
	v1.POST("/messages", h.sendText)
	v1.POST("/messages/media", h.sendMedia)
	v1.POST("/messages/poll", h.sendPoll)
	v1.POST("/uploads", h.upload)
	v1.POST("/chats/:id/read", h.markRead)
	v1.POST("/chats/:id/presence", h.subscribePresence)
	v1.GET("/devices/:id/groups", h.groups)
	v1.GET("/devices/:id/groups/:jid", h.groupParticipants)
	v1.POST("/campaigns/:id/:action", h.campaign)

	// Dibuka langsung oleh browser dengan token singkat dari bizbos.
	r.GET("/realtime", h.cors, h.realtime)
	r.GET("/media/:id", h.media)
	r.GET("/avatar/:id", h.avatar)
	return r
}

func (h *Handler) cors(c *gin.Context) {
	origin := c.GetHeader("Origin")
	if origin != "" && slices.Contains(h.Cfg.AllowedOrigins, origin) {
		c.Header("Access-Control-Allow-Origin", origin)
		c.Header("Vary", "Origin")
	}
	c.Next()
}

func ok(c *gin.Context, data any) {
	c.JSON(http.StatusOK, gin.H{"success": true, "data": data})
}

func fail(c *gin.Context, status int, msg string) {
	c.AbortWithStatusJSON(status, gin.H{"success": false, "message": msg})
}
