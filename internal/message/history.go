package message

import (
	"context"
	"log"
	"sort"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"go_bizbos/internal/store"
)

// Batas impor riwayat chat saat HP baru ditautkan, supaya database tidak membengkak.
const (
	historyMaxChats    = 300 // chat berbeda per device (selama proses berjalan)
	historyMaxMessages = 50  // pesan terakhir per chat
)

// webStatus: status pesan keluar di riwayat WhatsApp -> status bizbos.
var webStatus = map[waWeb.WebMessageInfo_Status]string{
	waWeb.WebMessageInfo_SERVER_ACK:   store.MsgSent,
	waWeb.WebMessageInfo_DELIVERY_ACK: store.MsgDelivered,
	waWeb.WebMessageInfo_READ:         store.MsgRead,
	waWeb.WebMessageInfo_PLAYED:       store.MsgPlayed,
}

// OnHistorySync menyimpan riwayat chat yang dikirim WhatsApp setelah HP ditautkan.
//
// Berbeda dengan OnMessage: pesan lama TIDAK dikirim ke webhook (bot tidak membalas chat lama),
// tidak dihitung sebagai opt-out, dan tidak menambah jumlah belum dibaca per pesan.
// Pesan yang sudah ada (wa_id sama) dilewati, jadi aman bila WhatsApp mengirim ulang.
func (in *Inbox) OnHistorySync(branch, device string, cli *whatsmeow.Client, e *events.HistorySync) {
	data := e.Data
	if data == nil {
		return
	}
	in.histMu.Lock()
	defer in.histMu.Unlock()
	if in.histChats == nil {
		in.histChats = map[string]map[string]bool{}
	}
	seen := in.histChats[device]
	if seen == nil {
		seen = map[string]bool{}
		in.histChats[device] = seen
	}
	ctx := context.Background()

	for _, pn := range data.GetPushnames() {
		jid, err := types.ParseJID(pn.GetID())
		if err != nil || pn.GetPushname() == "" || jid.Server != types.DefaultUserServer {
			continue
		}
		in.db.Model(&store.Chat{}).
			Where("device_id = ? AND chat_jid = ? AND (name IS NULL OR name = '')", device, jid.ToNonAD().String()).
			Update("name", pn.GetPushname())
	}

	chats, msgs := 0, 0
	for _, conv := range data.GetConversations() {
		// WhatsApp mengirim riwayat bertahap: chat yang sama bisa datang lagi dengan pesan lebih lama.
		// Batas dihitung per chat berbeda, jadi chat yang sudah masuk tetap boleh ditambah pesannya.
		if !seen[conv.GetID()] && len(seen) >= historyMaxChats {
			continue
		}
		if n := in.importConversation(ctx, branch, device, cli, conv); n > 0 {
			seen[conv.GetID()] = true
			chats++
			msgs += n
		}
	}
	if chats == 0 {
		return
	}
	log.Printf("[history] device %s: %d chat, %d pesan diimpor (%s)", device, chats, msgs, data.GetSyncType())
	// Hanya ke browser (bukan webhook): inbox yang sedang terbuka memuat ulang daftar chat.
	in.bus.Hub.Publish(branch, device, "history.sync", map[string]any{"device_id": device, "chats": chats, "messages": msgs})
}

func (in *Inbox) importConversation(ctx context.Context, branch, device string, cli *whatsmeow.Client, conv *waHistorySync.Conversation) int {
	raw, err := types.ParseJID(conv.GetID())
	if err != nil || raw.Server == types.BroadcastServer || raw.Server == types.NewsletterServer {
		return 0
	}
	group := raw.Server == types.GroupServer
	chatJID := raw.ToNonAD()
	if !group {
		alt := types.EmptyJID
		if pn, err := types.ParseJID(conv.GetPnJID()); err == nil && pn.Server == types.DefaultUserServer {
			alt = pn
		}
		chatJID = Normalize(ctx, cli, raw, alt)
	}

	evts := make([]*events.Message, 0, len(conv.GetMessages()))
	for _, hm := range conv.GetMessages() {
		evt, err := cli.ParseWebMessage(raw, hm.GetMessage())
		if err != nil || evt == nil || evt.Message == nil {
			continue
		}
		evts = append(evts, evt)
	}
	if len(evts) == 0 {
		return 0
	}
	sort.Slice(evts, func(i, j int) bool { return evts[i].Info.Timestamp.After(evts[j].Info.Timestamp) })
	if len(evts) > historyMaxMessages {
		evts = evts[:historyMaxMessages]
	}

	name := conv.GetName()
	if name == "" && !group {
		name = conv.GetDisplayName()
		for _, ev := range evts {
			if name != "" {
				break
			}
			if !ev.Info.IsFromMe && ev.Info.PushName != "" {
				name = ev.Info.PushName
			}
		}
	}
	chatID, err := in.ensureChat(branch, device, chatJID, name, group)
	if err != nil {
		return 0
	}

	saved := 0
	var newest *store.Message
	newestPreview := ""
	for _, ev := range evts {
		msg, prev, ok := in.historyMessage(ctx, branch, device, cli, chatID, ev)
		if !ok || !in.save(msg, branch, device, chatJID, name, group) {
			continue
		}
		saved++
		if newest == nil || msg.SentAt.After(newest.SentAt) {
			newest, newestPreview = msg, prev
		}
	}
	if newest != nil {
		in.db.Exec(`UPDATE wa_chats SET
			last_message_preview = IF(last_message_at IS NULL OR last_message_at <= ?, ?, last_message_preview),
			last_message_at = IF(last_message_at IS NULL OR last_message_at <= ?, ?, last_message_at),
			unread_count = GREATEST(unread_count, ?),
			updated_at = ?
			WHERE id = ?`, newest.SentAt, newestPreview, newest.SentAt, newest.SentAt, conv.GetUnreadCount(), time.Now(), newest.ChatID)
	}
	return saved
}

// historyMessage menyusun baris wa_messages dari pesan riwayat (sama dengan OnMessage, tanpa efek samping).
func (in *Inbox) historyMessage(ctx context.Context, branch, device string, cli *whatsmeow.Client, chatID string, e *events.Message) (*store.Message, string, bool) {
	if e.Message.GetPollUpdateMessage() != nil {
		return nil, "", false
	}
	p := parse(e.Message)
	switch p.Type {
	case "", "edit", "revoke":
		return nil, "", false
	}
	info := e.Info
	sender := Normalize(ctx, cli, info.Sender, info.SenderAlt)

	status := store.MsgDelivered
	if info.IsFromMe {
		status = store.MsgSent
		if s, ok := webStatus[e.SourceWebMsg.GetStatus()]; ok {
			status = s
		}
	}
	msg := &store.Message{
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
	return msg, preview(p), true
}
