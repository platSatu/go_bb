package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.mau.fi/whatsmeow/types"

	"go_bizbos/internal/auth"
	"go_bizbos/internal/device"
	"go_bizbos/internal/media"
	"go_bizbos/internal/sender"
	"go_bizbos/internal/store"
)

const maxText = 10000

var (
	targetRe = regexp.MustCompile(`^([1-9][0-9]{7,14}|[0-9]{5,25}@(s\.whatsapp\.net|lid)|[0-9-]{10,40}@g\.us)$`)
	refRe    = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)
)

func (h *Handler) connect(c *gin.Context) {
	var in struct {
		Phone string `json:"phone"`
	}
	_ = c.ShouldBindJSON(&in)
	code, mode, err := h.Devices.Connect(c.Request.Context(), auth.Branch(c), c.Param("id"), strings.TrimPrefix(strings.TrimSpace(in.Phone), "+"))
	if err != nil {
		fail(c, deviceStatus(err), err.Error())
		return
	}
	ok(c, gin.H{"mode": mode, "pairing_code": code, "message": pairingMessages[mode]})
}

var pairingMessages = map[string]string{
	device.ModeReconnect: "Menyambung ulang dengan sesi yang tersimpan, tidak perlu scan.",
	device.ModeQR:        "Pindai QR dari WhatsApp HP: Perangkat tertaut > Tautkan perangkat.",
	device.ModeCode:      "Masukkan kode ini di WhatsApp HP: Perangkat tertaut > Tautkan perangkat > Tautkan dengan nomor telepon.",
}

func (h *Handler) status(c *gin.Context) {
	d, err := h.Devices.Find(auth.Branch(c), c.Param("id"))
	if err != nil {
		fail(c, http.StatusNotFound, err.Error())
		return
	}
	qr, online := "", false
	if s := h.Devices.Session(d.ID); s != nil {
		qr, online = s.QR(), s.Client.IsConnected() && s.Client.IsLoggedIn()
	}
	ok(c, gin.H{"status": d.Status, "reason": d.StatusReason, "phone": d.Phone, "push_name": d.PushName, "online": online, "qr": qr, "banned_until": d.BannedUntil})
}

func (h *Handler) logout(c *gin.Context) {
	if err := h.Devices.Logout(c.Request.Context(), auth.Branch(c), c.Param("id")); err != nil {
		fail(c, deviceStatus(err), err.Error())
		return
	}
	ok(c, gin.H{"message": "Perangkat berhasil dikeluarkan dari WhatsApp."})
}

type sendInput struct {
	DeviceID   string `json:"device_id" form:"device_id"`
	To         string `json:"to" form:"to"`
	Text       string `json:"text" form:"text"`
	Type       string `form:"type"`
	QuotedWaID string `json:"quoted_wa_id" form:"quoted_wa_id"`
	ClientRef  string `json:"client_ref" form:"client_ref"`
}

// validate memastikan device milik branch ini dan isi pesan wajar.
func (h *Handler) validate(c *gin.Context, in *sendInput) (*store.Outbox, bool) {
	in.To = strings.TrimPrefix(strings.TrimSpace(in.To), "+")
	switch {
	case !auth.IsUUID(in.DeviceID):
		fail(c, http.StatusUnprocessableEntity, "Perangkat wajib dipilih.")
	case !targetRe.MatchString(in.To):
		fail(c, http.StatusUnprocessableEntity, "Nomor tujuan tidak valid. Gunakan format 628xxxxxxxxxx.")
	case len([]rune(in.Text)) > maxText:
		fail(c, http.StatusUnprocessableEntity, "Pesan terlalu panjang (maksimal 10.000 karakter).")
	case in.ClientRef != "" && !refRe.MatchString(in.ClientRef):
		fail(c, http.StatusUnprocessableEntity, "client_ref tidak valid.")
	case len(in.QuotedWaID) > 128:
		fail(c, http.StatusUnprocessableEntity, "Pesan yang dibalas tidak valid.")
	default:
		if _, err := h.Devices.Find(auth.Branch(c), in.DeviceID); err != nil {
			fail(c, http.StatusNotFound, err.Error())
			return nil, false
		}
		item := &store.Outbox{BranchID: auth.Branch(c), DeviceID: in.DeviceID, ChatJID: in.To, Type: "text"}
		if in.Text != "" {
			item.Body = store.Ptr(in.Text)
		}
		if in.QuotedWaID != "" {
			item.QuotedWaID = store.Ptr(in.QuotedWaID)
		}
		if in.ClientRef != "" {
			item.ClientRef = store.Ptr(in.ClientRef)
		}
		return item, true
	}
	return nil, false
}

func (h *Handler) sendText(c *gin.Context) {
	var in sendInput
	if c.ShouldBindJSON(&in) != nil {
		fail(c, http.StatusBadRequest, "Format data tidak valid.")
		return
	}
	if strings.TrimSpace(in.Text) == "" {
		fail(c, http.StatusUnprocessableEntity, "Pesan tidak boleh kosong.")
		return
	}
	item, valid := h.validate(c, &in)
	if !valid {
		return
	}
	h.enqueue(c, item)
}

func (h *Handler) sendMedia(c *gin.Context) {
	var in sendInput
	if c.ShouldBind(&in) != nil {
		fail(c, http.StatusBadRequest, "Format data tidak valid.")
		return
	}
	item, valid := h.validate(c, &in)
	if !valid {
		return
	}
	fh, err := c.FormFile("file")
	if err != nil {
		fail(c, http.StatusUnprocessableEntity, "File wajib dilampirkan.")
		return
	}
	up, kind, err := h.Media.SaveUpload(item.BranchID, fh)
	if err != nil {
		fail(c, uploadStatus(err), uploadMessage(err))
		return
	}
	if in.Type == "document" {
		kind = "document"
	}
	item.Type, item.UploadID = kind, &up.ID
	h.enqueue(c, item)
}

func (h *Handler) sendPoll(c *gin.Context) {
	var in struct {
		sendInput
		Question   string   `json:"question"`
		Options    []string `json:"options"`
		Selectable int      `json:"selectable_count"`
	}
	if c.ShouldBindJSON(&in) != nil {
		fail(c, http.StatusBadRequest, "Format data tidak valid.")
		return
	}
	in.Question = strings.TrimSpace(in.Question)
	options := make([]string, 0, len(in.Options))
	seen := map[string]bool{}
	for _, o := range in.Options {
		if o = strings.TrimSpace(o); o != "" && !seen[o] && len([]rune(o)) <= 100 {
			seen[o] = true
			options = append(options, o)
		}
	}
	switch {
	case in.Question == "" || len([]rune(in.Question)) > 255:
		fail(c, http.StatusUnprocessableEntity, "Pertanyaan wajib diisi (maksimal 255 karakter).")
		return
	case len(options) < 2 || len(options) > 12:
		fail(c, http.StatusUnprocessableEntity, "Polling butuh 2 sampai 12 pilihan jawaban yang berbeda.")
		return
	}
	in.Text = ""
	item, valid := h.validate(c, &in.sendInput)
	if !valid {
		return
	}
	payload, _ := json.Marshal(map[string]any{"options": options, "selectable": min(max(in.Selectable, 1), len(options))})
	item.Type, item.Body, item.Payload = "poll", &in.Question, store.Ptr(string(payload))
	h.enqueue(c, item)
}

func (h *Handler) enqueue(c *gin.Context, item *store.Outbox) {
	saved, err := h.Sender.Enqueue(c.Request.Context(), item)
	if err != nil {
		fail(c, http.StatusInternalServerError, "Pesan gagal dimasukkan ke antrean, silakan coba lagi.")
		return
	}
	ok(c, gin.H{"outbox_id": saved.ID, "status": saved.Status})
}

// upload: untuk lampiran blast. bizbos lalu menulis upload_id ke baris wa_outbox.
func (h *Handler) upload(c *gin.Context) {
	fh, err := c.FormFile("file")
	if err != nil {
		fail(c, http.StatusUnprocessableEntity, "File wajib dilampirkan.")
		return
	}
	up, kind, err := h.Media.SaveUpload(auth.Branch(c), fh)
	if err != nil {
		fail(c, uploadStatus(err), uploadMessage(err))
		return
	}
	ok(c, gin.H{"upload_id": up.ID, "kind": kind, "mime": up.Mime, "size": up.Size, "name": up.Name})
}

func (h *Handler) markRead(c *gin.Context) {
	var chat store.Chat
	if h.DB.First(&chat, "id = ? AND branch_id = ?", c.Param("id"), auth.Branch(c)).Error != nil {
		fail(c, http.StatusNotFound, "Chat tidak ditemukan.")
		return
	}
	var unread []store.Message
	h.DB.Select("id", "wa_id", "sender_jid").Where("chat_id = ? AND from_me = ? AND status <> ?", chat.ID, false, store.MsgRead).
		Order("sent_at desc").Limit(200).Find(&unread)

	if cli := h.Devices.Client(chat.DeviceID); cli != nil && len(unread) > 0 {
		if jid, err := types.ParseJID(chat.ChatJID); err == nil {
			bySender := map[string][]types.MessageID{}
			for _, m := range unread {
				if m.SenderJID != nil {
					bySender[*m.SenderJID] = append(bySender[*m.SenderJID], m.WaID)
				}
			}
			for sender, ids := range bySender {
				sjid, _ := types.ParseJID(sender)
				if !chat.IsGroup {
					sjid = jid
				}
				_ = cli.MarkRead(c.Request.Context(), ids, time.Now(), jid, sjid)
			}
		}
	}
	h.DB.Model(&store.Message{}).Where("chat_id = ? AND from_me = ? AND status <> ?", chat.ID, false, store.MsgRead).Update("status", store.MsgRead)
	h.DB.Model(&chat).Update("unread_count", 0)
	ok(c, gin.H{"chat_id": chat.ID})
}

func (h *Handler) campaign(c *gin.Context) {
	action := c.Param("action")
	if action != "start" && action != "pause" && action != "cancel" {
		fail(c, http.StatusNotFound, "Aksi tidak dikenal.")
		return
	}
	camp, err := h.Sender.Campaign(c.Request.Context(), auth.Branch(c), c.Param("id"), action)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, sender.ErrCampaignNotFound) {
			status = http.StatusNotFound
		} else if errors.Is(err, sender.ErrCampaignState) {
			status = http.StatusConflict
		}
		fail(c, status, err.Error())
		return
	}
	ok(c, gin.H{"campaign_id": camp.ID, "status": camp.Status, "total": camp.Total})
}

func deviceStatus(err error) int {
	switch {
	case errors.Is(err, device.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, device.ErrAlreadyLinked):
		return http.StatusConflict
	case errors.Is(err, device.ErrBadPhone):
		return http.StatusUnprocessableEntity
	}
	return http.StatusBadGateway
}

func uploadStatus(err error) int {
	if errors.Is(err, media.ErrTooLarge) {
		return http.StatusRequestEntityTooLarge
	}
	return http.StatusInternalServerError
}

func uploadMessage(err error) string {
	if errors.Is(err, media.ErrTooLarge) {
		return err.Error()
	}
	return "File gagal disimpan, silakan coba lagi."
}
