package notify

import (
	"go_bizbos/internal/realtime"
	"go_bizbos/internal/webhook"
)

const (
	DeviceStatus   = "device.status"
	DeviceQR       = "device.qr"
	MessageNew     = "message.new"
	MessageStatus  = "message.status"
	MessageMedia   = "message.media"
	OutboxStatus   = "outbox.status"
	CampaignStatus = "campaign.status"
	Presence       = "presence"
	PollVote       = "poll.vote"
)

// Event yang juga diteruskan ke bizbos lewat webhook (QR cukup lewat SSE).
var toWebhook = map[string]bool{DeviceStatus: true, MessageNew: true, MessageStatus: true, CampaignStatus: true}

type Bus struct {
	Hub     *realtime.Hub
	Webhook *webhook.Outbox
}

func (b *Bus) Emit(branch, device, name string, data map[string]any) {
	data["device_id"] = device
	b.Hub.Publish(branch, device, name, data)
	if toWebhook[name] {
		b.Webhook.Enqueue(branch, name, data)
	}
}
