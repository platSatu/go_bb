package sender

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"

	"go_bizbos/internal/media"
	"go_bizbos/internal/store"
)

type built struct {
	msg    *waE2E.Message
	kind   string
	upload *store.Upload
	poll   []string
}

type cachedUpload struct {
	resp whatsmeow.UploadResponse
	at   time.Time
}

// Satu file blast cukup diunggah sekali per device, lalu dipakai ulang untuk semua penerima.
var uploads sync.Map

func (s *Sender) build(ctx context.Context, cli *whatsmeow.Client, item *store.Outbox) (*built, error) {
	body := ""
	if item.Body != nil {
		body = *item.Body
	}
	ctxInfo := s.quote(item)

	if item.Type == "text" {
		if ctxInfo == nil {
			return &built{msg: &waE2E.Message{Conversation: proto.String(body)}, kind: "text"}, nil
		}
		return &built{msg: &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String(body), ContextInfo: ctxInfo}}, kind: "text"}, nil
	}

	if item.Type == "poll" {
		var p struct {
			Options    []string `json:"options"`
			Selectable int      `json:"selectable"`
		}
		if item.Payload == nil || json.Unmarshal([]byte(*item.Payload), &p) != nil || len(p.Options) < 2 {
			return nil, errBadPoll
		}
		return &built{msg: cli.BuildPollCreation(body, p.Options, max(p.Selectable, 1)), kind: "poll", poll: p.Options}, nil
	}

	if item.UploadID == nil {
		return nil, errNoUpload
	}
	var up store.Upload
	if s.db.First(&up, "id = ? AND branch_id = ?", *item.UploadID, item.BranchID).Error != nil {
		return nil, errNoUpload
	}
	kind := item.Type
	if kind != "document" && media.KindOf(up.Mime) != kind {
		kind = "document" // isi file tidak cocok dengan tipe yang diminta, kirim sebagai dokumen
	}

	resp, err := s.uploadOnce(ctx, cli, item.DeviceID, &up, kind)
	if err != nil {
		return nil, err
	}
	size := proto.Uint64(resp.FileLength)
	b := &built{kind: kind, upload: &up, msg: &waE2E.Message{}}
	switch kind {
	case "image":
		b.msg.ImageMessage = &waE2E.ImageMessage{URL: &resp.URL, DirectPath: &resp.DirectPath, MediaKey: resp.MediaKey, Mimetype: &up.Mime, FileEncSHA256: resp.FileEncSHA256, FileSHA256: resp.FileSHA256, FileLength: size, Caption: proto.String(body), ContextInfo: ctxInfo}
	case "video":
		b.msg.VideoMessage = &waE2E.VideoMessage{URL: &resp.URL, DirectPath: &resp.DirectPath, MediaKey: resp.MediaKey, Mimetype: &up.Mime, FileEncSHA256: resp.FileEncSHA256, FileSHA256: resp.FileSHA256, FileLength: size, Caption: proto.String(body), ContextInfo: ctxInfo}
	case "audio":
		b.msg.AudioMessage = &waE2E.AudioMessage{URL: &resp.URL, DirectPath: &resp.DirectPath, MediaKey: resp.MediaKey, Mimetype: &up.Mime, FileEncSHA256: resp.FileEncSHA256, FileSHA256: resp.FileSHA256, FileLength: size, ContextInfo: ctxInfo}
	default:
		b.msg.DocumentMessage = &waE2E.DocumentMessage{URL: &resp.URL, DirectPath: &resp.DirectPath, MediaKey: resp.MediaKey, Mimetype: &up.Mime, FileEncSHA256: resp.FileEncSHA256, FileSHA256: resp.FileSHA256, FileLength: size, FileName: &up.Name, Title: &up.Name, Caption: proto.String(body), ContextInfo: ctxInfo}
	}
	return b, nil
}

func (s *Sender) uploadOnce(ctx context.Context, cli *whatsmeow.Client, deviceID string, up *store.Upload, kind string) (whatsmeow.UploadResponse, error) {
	key := deviceID + "|" + up.ID + "|" + kind
	if v, ok := uploads.Load(key); ok && time.Since(v.(cachedUpload).at) < time.Hour {
		return v.(cachedUpload).resp, nil
	}
	f, err := s.media.Open(up)
	if err != nil {
		return whatsmeow.UploadResponse{}, errNoUpload
	}
	defer f.Close()

	uctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	resp, err := cli.UploadReader(uctx, f, nil, media.WaType(kind))
	if err != nil {
		return resp, err
	}
	uploads.Store(key, cachedUpload{resp: resp, at: time.Now()})
	return resp, nil
}

func (s *Sender) quote(item *store.Outbox) *waE2E.ContextInfo {
	if item.QuotedWaID == nil {
		return nil
	}
	var q store.Message
	if s.db.First(&q, "device_id = ? AND wa_id = ?", item.DeviceID, *item.QuotedWaID).Error != nil {
		return nil
	}
	info := &waE2E.ContextInfo{StanzaID: &q.WaID, QuotedMessage: &waE2E.Message{Conversation: proto.String(derefStr(q.Body))}}
	if q.SenderJID != nil {
		info.Participant = q.SenderJID
	}
	return info
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
