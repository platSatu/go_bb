package api

import (
	"context"
	"net/http"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

	"go_bizbos/internal/auth"
	"go_bizbos/internal/store"
)

// readyClient: device milik branch ini dan sedang online.
func (h *Handler) readyClient(c *gin.Context, deviceID string) *whatsmeow.Client {
	if _, err := h.Devices.Find(auth.Branch(c), deviceID); err != nil {
		fail(c, http.StatusNotFound, err.Error())
		return nil
	}
	cli := h.Devices.Client(deviceID)
	if cli == nil {
		fail(c, http.StatusServiceUnavailable, "Perangkat sedang tidak terhubung ke WhatsApp.")
	}
	return cli
}

// subscribePresence: minta WhatsApp mengabari status online/mengetik kontak (lewat SSE "presence").
func (h *Handler) subscribePresence(c *gin.Context) {
	var chat store.Chat
	if h.DB.First(&chat, "id = ? AND branch_id = ?", c.Param("id"), auth.Branch(c)).Error != nil {
		fail(c, http.StatusNotFound, "Chat tidak ditemukan.")
		return
	}
	cli := h.readyClient(c, chat.DeviceID)
	if cli == nil {
		return
	}
	jid, err := types.ParseJID(chat.ChatJID)
	if err != nil || chat.IsGroup {
		ok(c, gin.H{"subscribed": false})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()
	ok(c, gin.H{"subscribed": cli.SubscribePresence(ctx, jid) == nil})
}

func (h *Handler) groups(c *gin.Context) {
	cli := h.readyClient(c, c.Param("id"))
	if cli == nil {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()
	list, err := cli.GetJoinedGroups(ctx)
	if err != nil {
		fail(c, http.StatusBadGateway, "Gagal mengambil daftar grup dari WhatsApp, silakan coba lagi.")
		return
	}
	own := cli.Store.ID.ToNonAD()
	out := make([]gin.H, 0, len(list))
	for _, g := range list {
		admin := false
		for _, p := range g.Participants {
			if (p.PhoneNumber.User == own.User || p.JID.User == own.User) && (p.IsAdmin || p.IsSuperAdmin) {
				admin = true
			}
		}
		out = append(out, gin.H{"jid": g.JID.String(), "name": g.Name, "participants": len(g.Participants), "is_admin": admin, "announce": g.IsAnnounce, "created_at": g.GroupCreated})
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["name"].(string) < out[j]["name"].(string) })
	ok(c, gin.H{"groups": out})
}

func (h *Handler) groupParticipants(c *gin.Context) {
	cli := h.readyClient(c, c.Param("id"))
	if cli == nil {
		return
	}
	jid, err := types.ParseJID(c.Param("jid"))
	if err != nil || jid.Server != types.GroupServer {
		fail(c, http.StatusUnprocessableEntity, "Grup tidak valid.")
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 20*time.Second)
	defer cancel()
	g, err := cli.GetGroupInfo(ctx, jid)
	if err != nil {
		fail(c, http.StatusBadGateway, "Gagal mengambil anggota grup, pastikan nomor ini masih anggota grup tersebut.")
		return
	}
	members := make([]gin.H, 0, len(g.Participants))
	for _, p := range g.Participants {
		phone := p.PhoneNumber.User
		if phone == "" && p.JID.Server == types.DefaultUserServer {
			phone = p.JID.User
		}
		if phone == "" {
			if pn, err := cli.Store.LIDs.GetPNForLID(ctx, p.JID); err == nil {
				phone = pn.User
			}
		}
		members = append(members, gin.H{"phone": phone, "name": p.DisplayName, "is_admin": p.IsAdmin || p.IsSuperAdmin})
	}
	ok(c, gin.H{"jid": g.JID.String(), "name": g.Name, "participants": members})
}
