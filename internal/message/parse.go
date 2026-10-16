package message

import (
	"fmt"
	"strings"

	"go.mau.fi/whatsmeow/proto/waE2E"

	"go_bizbos/internal/media"
)

// Parsed: isi pesan yang kita simpan. Media tidak diunduh di sini (lazy).
type Parsed struct {
	Type   string
	Body   string
	Quoted string
	Media  *media.Meta
	Mime   string
	Name   string
	Size   int64
	Poll   []string // pilihan jawaban polling

	EditOf   string // id pesan yang diedit
	RevokeOf string // id pesan yang ditarik
}

type downloadable interface {
	GetDirectPath() string
	GetMediaKey() []byte
	GetFileSHA256() []byte
	GetFileEncSHA256() []byte
	GetMimetype() string
	GetFileLength() uint64
}

func parse(m *waE2E.Message) Parsed {
	var p Parsed
	setMedia := func(kind string, d downloadable, caption string) {
		p.Type, p.Body, p.Mime, p.Size = kind, caption, d.GetMimetype(), int64(d.GetFileLength())
		p.Media = &media.Meta{Kind: kind, DirectPath: d.GetDirectPath(), MediaKey: d.GetMediaKey(), SHA256: d.GetFileSHA256(), EncSHA256: d.GetFileEncSHA256()}
	}

	switch {
	case m.GetConversation() != "":
		p.Type, p.Body = "text", m.GetConversation()
	case m.GetExtendedTextMessage() != nil:
		x := m.GetExtendedTextMessage()
		p.Type, p.Body, p.Quoted = "text", x.GetText(), x.GetContextInfo().GetStanzaID()
	case m.GetImageMessage() != nil:
		x := m.GetImageMessage()
		setMedia("image", x, x.GetCaption())
		p.Quoted = x.GetContextInfo().GetStanzaID()
	case m.GetVideoMessage() != nil:
		x := m.GetVideoMessage()
		setMedia("video", x, x.GetCaption())
		p.Quoted = x.GetContextInfo().GetStanzaID()
	case m.GetPtvMessage() != nil:
		setMedia("video", m.GetPtvMessage(), "")
	case m.GetAudioMessage() != nil:
		setMedia("audio", m.GetAudioMessage(), "")
	case m.GetDocumentMessage() != nil:
		x := m.GetDocumentMessage()
		setMedia("document", x, x.GetCaption())
		p.Name = x.GetFileName()
		p.Quoted = x.GetContextInfo().GetStanzaID()
	case m.GetStickerMessage() != nil:
		setMedia("sticker", m.GetStickerMessage(), "")
	case m.GetLocationMessage() != nil:
		x := m.GetLocationMessage()
		p.Type = "location"
		p.Body = strings.TrimSpace(fmt.Sprintf("%s %s\nhttps://maps.google.com/?q=%f,%f", x.GetName(), x.GetAddress(), x.GetDegreesLatitude(), x.GetDegreesLongitude()))
	case m.GetContactMessage() != nil:
		p.Type, p.Body = "contact", m.GetContactMessage().GetDisplayName()
	case m.GetReactionMessage() != nil:
		x := m.GetReactionMessage()
		p.Type, p.Body, p.Quoted = "reaction", x.GetText(), x.GetKey().GetID()
	case m.GetPollCreationMessage() != nil || m.GetPollCreationMessageV3() != nil:
		x := m.GetPollCreationMessage()
		if x == nil {
			x = m.GetPollCreationMessageV3()
		}
		p.Type, p.Body = "poll", x.GetName()
		for _, o := range x.GetOptions() {
			p.Poll = append(p.Poll, o.GetOptionName())
		}
	case m.GetProtocolMessage() != nil:
		x := m.GetProtocolMessage()
		switch x.GetType() {
		case waE2E.ProtocolMessage_REVOKE:
			p.Type, p.RevokeOf = "revoke", x.GetKey().GetID()
		case waE2E.ProtocolMessage_MESSAGE_EDIT:
			inner := parse(x.GetEditedMessage())
			p.Type, p.Body, p.EditOf = "edit", inner.Body, x.GetKey().GetID()
		}
	}
	return p
}

func preview(p Parsed) string {
	labels := map[string]string{"image": "📷 Foto", "video": "🎥 Video", "audio": "🎤 Pesan suara", "document": "📄 Dokumen", "sticker": "Stiker", "location": "📍 Lokasi", "contact": "👤 Kontak", "poll": "📊 Polling"}
	text := p.Body
	if l, ok := labels[p.Type]; ok && (text == "" || p.Type == "document") {
		text = l
		if p.Name != "" {
			text += " " + p.Name
		}
	}
	if r := []rune(text); len(r) > 120 {
		text = string(r[:120]) + "…"
	}
	return text
}
