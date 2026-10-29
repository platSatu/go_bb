package message

import (
	"context"
	"log"
	"strings"
	"sync"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

	"go_bizbos/internal/store"
)

var nameMu sync.Mutex // pengisian nama dijalankan satu per satu

// FillNames mengisi nama chat (bukan grup) yang masih kosong dari kontak WhatsApp di HP:
// nama yang disimpan di HP, lalu nama bisnis, lalu nama profil. Nama yang sudah ada tidak ditimpa.
// Dipanggil saat device tersambung, setelah impor riwayat, dan setelah sinkron kontak selesai.
func (in *Inbox) FillNames(device string, cli *whatsmeow.Client) {
	if cli == nil || cli.Store == nil || cli.Store.Contacts == nil {
		return
	}
	nameMu.Lock()
	defer nameMu.Unlock()

	var chats []store.Chat
	err := in.db.Select("id", "chat_jid").
		Where("device_id = ? AND is_group = ? AND (name IS NULL OR name = '')", device, false).
		Find(&chats).Error
	if err != nil || len(chats) == 0 {
		return
	}
	ctx := context.Background()
	contacts, err := cli.Store.Contacts.GetAllContacts(ctx)
	if err != nil || len(contacts) == 0 {
		return
	}

	filled := 0
	for _, c := range chats {
		jid, err := types.ParseJID(c.ChatJID)
		if err != nil {
			continue
		}
		jid = jid.ToNonAD()
		name := contactName(contacts[jid])
		if name == "" {
			// Kontak bisa tersimpan dengan nomor HP atau dengan @lid; coba pasangannya.
			var other types.JID
			if jid.Server == types.DefaultUserServer {
				other, _ = cli.Store.LIDs.GetLIDForPN(ctx, jid)
			} else if jid.Server == types.HiddenUserServer {
				other, _ = cli.Store.LIDs.GetPNForLID(ctx, jid)
			}
			if !other.IsEmpty() {
				name = contactName(contacts[other.ToNonAD()])
			}
		}
		if name == "" {
			continue
		}
		res := in.db.Model(&store.Chat{}).Where("id = ? AND (name IS NULL OR name = '')", c.ID).Update("name", name)
		if res.RowsAffected > 0 {
			filled++
		}
	}
	if filled > 0 {
		log.Printf("[names] device %s: %d nama chat diisi dari kontak", device, filled)
	}
}

func contactName(c types.ContactInfo) string {
	for _, n := range []string{c.FullName, c.FirstName, c.BusinessName, c.PushName} {
		if n = strings.TrimSpace(n); n != "" {
			return n
		}
	}
	return ""
}
