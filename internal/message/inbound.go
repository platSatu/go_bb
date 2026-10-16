package message

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"go_bizbos/internal/notify"
	"go_bizbos/internal/store"
)

var optOutWords = map[string]bool{"STOP": true, "BERHENTI": true, "UNSUBSCRIBE": true}

type Inbox struct {
	db    *gorm.DB
	bus   *notify.Bus
	chats sync.Map // device_id|jid -> chat_id
}

func NewInbox(db *gorm.DB, bus *notify.Bus) *Inbox { return &Inbox{db: db, bus: bus} }

// Normalize mengubah JID @lid menjadi nomor HP bila pemetaannya diketahui, supaya satu kontak = satu chat.
func Normalize(ctx context.Context, cli *whatsmeow.Client, jid, alt types.JID) types.JID {
	jid = jid.ToNonAD()
	if jid.Server != types.HiddenUserServer {
		return jid
	}
	if alt.Server == types.DefaultUserServer {
		return alt.ToNonAD()
	}
	if pn, err := cli.Store.LIDs.GetPNForLID(ctx, jid); err == nil && !pn.IsEmpty() {
		return pn.ToNonAD()
	}
	return jid
}

func (in *Inbox) OnMessage(branch, device string, cli *whatsmeow.Client, e *events.Message) {
	info := e.Info
	if info.Chat.Server == types.BroadcastServer || info.Chat.Server == types.NewsletterServer {
		return // status & saluran tidak masuk inbox
	}
	ctx := context.Background()
	if e.Message.GetPollUpdateMessage() != nil {
		in.onPollVote(ctx, branch, device, cli, e)
		return
	}
	p := parse(e.Message)
	if p.Type == "" {
		return
	}

	alt := info.SenderAlt
	if info.IsFromMe {
		alt = info.RecipientAlt
	}
	chatJID := info.Chat.ToNonAD()
	if !info.IsGroup {
		chatJID = Normalize(ctx, cli, info.Chat, alt)
	}
	sender := Normalize(ctx, cli, info.Sender, info.SenderAlt)

	switch p.Type {
	case "edit":
		in.db.Model(&store.Message{}).Where("device_id = ? AND wa_id = ?", device, p.EditOf).Update("body", p.Body)
		in.bus.Emit(branch, device, notify.MessageStatus, map[string]any{"chat_jid": chatJID.String(), "wa_ids": []string{p.EditOf}, "edited": p.Body})
		return
	case "revoke":
		in.db.Model(&store.Message{}).Where("device_id = ? AND wa_id = ?", device, p.RevokeOf).Updates(map[string]any{"type": "revoked", "body": nil})
		in.bus.Emit(branch, device, notify.MessageStatus, map[string]any{"chat_jid": chatJID.String(), "wa_ids": []string{p.RevokeOf}, "revoked": true})
		return
	}

	var name string
	if info.IsGroup {
		name = in.groupName(ctx, cli, device, chatJID)
	} else if !info.IsFromMe {
		name = info.PushName
	}
	chatID, err := in.ensureChat(branch, device, chatJID, name, info.IsGroup)
	if err != nil {
		return
	}

	status := store.MsgDelivered
	if info.IsFromMe {
		status = store.MsgSent
	}
	msg := store.Message{
		ID: store.NewID(), BranchID: branch, DeviceID: device, ChatID: chatID, WaID: info.ID, FromMe: info.IsFromMe,
		SenderJID: store.Ptr(sender.String()), Type: p.Type, Status: status, SentAt: info.Timestamp,
	}
	if info.PushName != "" && !info.IsFromMe {
		msg.SenderName = store.Ptr(info.PushName)
	}
	if p.Body != "" {
		msg.Body = store.Ptr(p.Body)
	}
	if p.Quoted != "" {
		msg.QuotedWaID = store.Ptr(p.Quoted)
	}
	if p.Media != nil {
		p.Media.Chat, p.Media.Sender, p.Media.IsGroup = info.Chat.String(), info.Sender.String(), info.IsGroup
		msg.MediaMeta, msg.MediaMime, msg.MediaSize = p.Media.JSON(), store.Ptr(p.Mime), store.Ptr(p.Size)
		if p.Name != "" {
			msg.MediaName = store.Ptr(p.Name)
		}
	}
	if len(p.Poll) > 0 {
		msg.Meta = PollMeta(p.Poll)
	}
	if !in.save(&msg, branch, device, chatJID, name, info.IsGroup) {
		return // duplikat (pesan dikirim ulang oleh WhatsApp) atau gagal simpan
	}
	in.touchChat(msg.ChatID, &msg, preview(p))

	if !info.IsFromMe && !info.IsGroup && optOutWords[strings.ToUpper(strings.TrimSpace(p.Body))] {
		in.db.Exec("INSERT IGNORE INTO wa_optouts (id, branch_id, phone, created_at) VALUES (?, ?, ?, ?)", store.NewID(), branch, chatJID.User, time.Now())
	}

	in.emitNew(&msg, chatJID, preview(p), "", false)
}

// SaveOutgoing dipakai sender setelah pesan terkirim dari bizbos.
func (in *Inbox) SaveOutgoing(msg *store.Message, chatJID types.JID, previewText, clientRef string) bool {
	chatID, err := in.ensureChat(msg.BranchID, msg.DeviceID, chatJID, "", chatJID.Server == types.GroupServer)
	if err != nil {
		return false
	}
	msg.ChatID = chatID
	if !in.save(msg, msg.BranchID, msg.DeviceID, chatJID, "", chatJID.Server == types.GroupServer) {
		return false
	}
	in.touchChat(msg.ChatID, msg, previewText)
	in.emitNew(msg, chatJID, previewText, clientRef, true)
	return true
}

// emitNew: isi pesan dikirim lengkap supaya inbox bisa langsung menampilkan tanpa memuat ulang.
// outbox = true bila dikirim lewat bizbos (inbox/blast/bot/API), false bila dari HP atau pesan masuk.
func (in *Inbox) emitNew(msg *store.Message, chatJID types.JID, previewText, clientRef string, outbox bool) {
	in.bus.Emit(msg.BranchID, msg.DeviceID, notify.MessageNew, map[string]any{
		"chat_id": msg.ChatID, "chat_jid": chatJID.String(), "preview": previewText, "client_ref": clientRef, "outbox": outbox, "is_group": chatJID.Server == types.GroupServer,
		"message": map[string]any{
			"id": msg.ID, "wa_id": msg.WaID, "from_me": msg.FromMe, "type": msg.Type, "body": msg.Body,
			"status": msg.Status, "sent_at": msg.SentAt.Format(time.RFC3339), "sender_name": msg.SenderName,
			"has_media": msg.MediaMeta != nil || msg.MediaPath != nil, "media_name": msg.MediaName,
			"quoted_wa_id": msg.QuotedWaID, "meta": msg.Meta,
		},
	})
}

func PollMeta(options []string) *string {
	b, _ := json.Marshal(map[string]any{"options": options})
	s := string(b)
	return &s
}

// onPollVote: suara polling dienkripsi; dibuka lalu dicocokkan dengan hash pilihan jawaban.
func (in *Inbox) onPollVote(ctx context.Context, branch, device string, cli *whatsmeow.Client, e *events.Message) {
	key := e.Message.GetPollUpdateMessage().GetPollCreationMessageKey()
	var poll store.Message
	if in.db.Where("device_id = ? AND wa_id = ? AND type = ?", device, key.GetID(), "poll").First(&poll).Error != nil || poll.Meta == nil {
		return
	}
	vote, err := cli.DecryptPollVote(ctx, e)
	if err != nil {
		return
	}
	var meta struct {
		Options []string `json:"options"`
	}
	if json.Unmarshal([]byte(*poll.Meta), &meta) != nil {
		return
	}
	hashes := whatsmeow.HashPollOptions(meta.Options)
	selected := []string{}
	for _, h := range vote.GetSelectedOptions() {
		for i, oh := range hashes {
			if bytes.Equal(h, oh) {
				selected = append(selected, meta.Options[i])
			}
		}
	}
	data, _ := json.Marshal(selected)
	voter := Normalize(ctx, cli, e.Info.Sender, e.Info.SenderAlt).String()
	row := store.PollVote{ID: store.NewID(), MessageID: poll.ID, VoterJID: voter, SelectedOptions: string(data), VotedAt: e.Info.Timestamp}
	in.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "message_id"}, {Name: "voter_jid"}},
		DoUpdates: clause.AssignmentColumns([]string{"selected_options", "voted_at", "updated_at"}),
	}).Create(&row)
	in.bus.Emit(branch, device, notify.PollVote, map[string]any{"message_id": poll.ID, "chat_id": poll.ChatID})
}

// OnPresence: status online/mengetik kontak (hanya terkirim setelah SubscribePresence).
func (in *Inbox) OnPresence(branch, device string, cli *whatsmeow.Client, evt any) {
	ctx := context.Background()
	data := map[string]any{}
	switch e := evt.(type) {
	case *events.Presence:
		data["chat_jid"] = Normalize(ctx, cli, e.From, types.EmptyJID).String()
		data["state"] = "online"
		if e.Unavailable {
			data["state"] = "offline"
		}
		if !e.LastSeen.IsZero() {
			data["last_seen"] = e.LastSeen.Format(time.RFC3339)
		}
	case *events.ChatPresence:
		if e.IsGroup {
			return
		}
		data["chat_jid"] = Normalize(ctx, cli, e.Chat, e.SenderAlt).String()
		data["state"] = "online"
		if e.State == types.ChatPresenceComposing {
			data["state"] = "typing"
		}
	default:
		return
	}
	in.bus.Emit(branch, device, notify.Presence, data)
}

// save menyimpan pesan; bila chat di cache sudah dihapus dari bizbos, chat dibuat ulang sekali.
func (in *Inbox) save(msg *store.Message, branch, device string, jid types.JID, name string, group bool) bool {
	for try := 0; try < 2; try++ {
		res := in.db.Clauses(clause.OnConflict{DoNothing: true}).Create(msg)
		if res.Error == nil {
			return res.RowsAffected > 0
		}
		in.chats.Delete(device + "|" + jid.String())
		id, err := in.ensureChat(branch, device, jid, name, group)
		if err != nil {
			return false
		}
		msg.ChatID = id
	}
	return false
}

func (in *Inbox) ensureChat(branch, device string, jid types.JID, name string, group bool) (string, error) {
	key := device + "|" + jid.String()
	if id, ok := in.chats.Load(key); ok {
		if name != "" {
			in.db.Model(&store.Chat{}).Where("id = ? AND (name IS NULL OR name = '')", id).Update("name", name)
		}
		return id.(string), nil
	}

	chat := store.Chat{ID: store.NewID(), BranchID: branch, DeviceID: device, ChatJID: jid.String(), IsGroup: group}
	if !group && jid.Server == types.DefaultUserServer {
		chat.Phone = store.Ptr(jid.User)
	}
	if name != "" {
		chat.Name = store.Ptr(name)
	}
	if err := in.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&chat).Error; err != nil {
		return "", err
	}
	var id string
	if err := in.db.Model(&store.Chat{}).Where("device_id = ? AND chat_jid = ?", device, jid.String()).Pluck("id", &id).Error; err != nil || id == "" {
		return "", gorm.ErrRecordNotFound
	}
	in.chats.Store(key, id)
	return id, nil
}

func (in *Inbox) touchChat(chatID string, msg *store.Message, previewText string) {
	unread := 0
	if !msg.FromMe {
		unread = 1
	}
	in.db.Exec(`UPDATE wa_chats SET
		unread_count = unread_count + ?,
		last_message_preview = IF(last_message_at IS NULL OR last_message_at <= ?, ?, last_message_preview),
		last_message_at = IF(last_message_at IS NULL OR last_message_at <= ?, ?, last_message_at),
		updated_at = ?
		WHERE id = ?`, unread, msg.SentAt, previewText, msg.SentAt, msg.SentAt, time.Now(), chatID)
}

func (in *Inbox) groupName(ctx context.Context, cli *whatsmeow.Client, device string, jid types.JID) string {
	if _, ok := in.chats.Load(device + "|" + jid.String()); ok {
		return ""
	}
	gctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if g, err := cli.GetGroupInfo(gctx, jid); err == nil {
		return g.Name
	}
	return ""
}

// OnReceipt memajukan status pesan keluar (terkirim -> diterima -> dibaca).
func (in *Inbox) OnReceipt(branch, device string, cli *whatsmeow.Client, e *events.Receipt) {
	ctx := context.Background()
	if e.Type == types.ReceiptTypeReadSelf {
		chat := Normalize(ctx, cli, e.Chat, e.RecipientAlt)
		in.db.Model(&store.Chat{}).Where("device_id = ? AND chat_jid = ?", device, chat.String()).Update("unread_count", 0)
		return
	}

	status := map[types.ReceiptType]string{types.ReceiptTypeDelivered: store.MsgDelivered, types.ReceiptTypeRead: store.MsgRead, types.ReceiptTypePlayed: store.MsgPlayed}[e.Type]
	if status == "" || len(e.MessageIDs) == 0 {
		return
	}
	var lower []string
	for s, r := range store.MsgRank {
		if r < store.MsgRank[status] {
			lower = append(lower, s)
		}
	}
	res := in.db.Model(&store.Message{}).
		Where("device_id = ? AND from_me = ? AND wa_id IN ? AND status IN ?", device, true, e.MessageIDs, lower).
		Update("status", status)
	if res.RowsAffected > 0 {
		chat := e.Chat.ToNonAD()
		if !e.IsGroup {
			chat = Normalize(ctx, cli, e.Chat, e.SenderAlt)
		}
		in.bus.Emit(branch, device, notify.MessageStatus, map[string]any{"chat_jid": chat.String(), "wa_ids": e.MessageIDs, "status": status})
	}
}
