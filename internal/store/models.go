package store

import "time"

// Tabel dibuat oleh migration Laravel (app.bizbos). Go tidak pernah AutoMigrate.

const (
	DeviceDisconnected = "disconnected"
	DeviceConnecting   = "connecting"
	DevicePairing      = "pairing"
	DeviceConnected    = "connected"
	DeviceLoggedOut    = "logged_out"
	DeviceBanned       = "banned"
	DeviceReplaced     = "replaced"
)

const (
	MsgPending   = "pending"
	MsgSent      = "sent"
	MsgDelivered = "delivered"
	MsgRead      = "read"
	MsgPlayed    = "played"
	MsgFailed    = "failed"
)

// Urutan status pesan keluar; status hanya boleh maju.
var MsgRank = map[string]int{MsgPending: 0, MsgSent: 1, MsgDelivered: 2, MsgRead: 3, MsgPlayed: 4}

const (
	OutQueued    = "queued"
	OutSending   = "sending"
	OutSent      = "sent"
	OutFailed    = "failed"
	OutSkipped   = "skipped"
	OutCancelled = "cancelled"
)

const (
	PriorityManual = 0
	PriorityBlast  = 10
)

const (
	CampaignDraft     = "draft"
	CampaignRunning   = "running"
	CampaignPaused    = "paused"
	CampaignDone      = "done"
	CampaignCancelled = "cancelled"
)

type Device struct {
	ID              string `gorm:"primaryKey"`
	BranchID        string
	Name            string
	Phone           *string
	JID             *string `gorm:"column:jid"`
	PushName        *string
	Platform        *string
	Status          string
	StatusReason    *string
	DailyCap        int
	SentToday       int
	SentDate        *string `gorm:"column:sent_date"`
	WarmupStartedAt *time.Time
	BannedUntil     *time.Time
	LastConnectedAt *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

func (Device) TableName() string { return "wa_devices" }

type Chat struct {
	ID                 string `gorm:"primaryKey"`
	BranchID           string
	DeviceID           string
	ChatJID            string `gorm:"column:chat_jid"`
	Phone              *string
	Name               *string
	IsGroup            bool
	UnreadCount        int
	LastMessageAt      *time.Time
	LastMessagePreview *string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

func (Chat) TableName() string { return "wa_chats" }

type Message struct {
	ID         string `gorm:"primaryKey"`
	BranchID   string
	DeviceID   string
	ChatID     string
	WaID       string `gorm:"column:wa_id"`
	FromMe     bool
	SenderJID  *string `gorm:"column:sender_jid"`
	SenderName *string
	Type       string
	Body       *string
	QuotedWaID *string `gorm:"column:quoted_wa_id"`
	MediaMeta  *string // JSON MediaMeta, untuk unduh lambat
	MediaPath  *string
	MediaMime  *string
	MediaName  *string
	MediaSize  *int64
	Meta       *string // JSON tambahan, mis. {"options": [...]} untuk polling
	Status     string
	Error      *string
	SentAt     time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func (Message) TableName() string { return "wa_messages" }

type Upload struct {
	ID        string `gorm:"primaryKey"`
	BranchID  string
	Path      string
	Mime      string
	Name      string
	Size      int64
	CreatedAt time.Time
}

func (Upload) TableName() string { return "wa_uploads" }

type Outbox struct {
	ID          string `gorm:"primaryKey"`
	BranchID    string
	DeviceID    string
	CampaignID  *string
	ChatJID     string `gorm:"column:chat_jid"`
	Type        string
	Body        *string
	UploadID    *string
	QuotedWaID  *string `gorm:"column:quoted_wa_id"`
	Payload     *string // JSON tambahan, mis. pilihan polling
	Priority    int
	Status      string
	Attempts    int
	AvailableAt time.Time
	MessageID   *string
	Error       *string
	ClientRef   *string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func (Outbox) TableName() string { return "wa_outbox" }

type Campaign struct {
	ID         string `gorm:"primaryKey"`
	BranchID   string
	DeviceID   string
	Name       string
	Status     string
	Total      int
	Sent       int
	Failed     int
	StartedAt  *time.Time
	FinishedAt *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func (Campaign) TableName() string { return "wa_campaigns" }

type WebhookEvent struct {
	ID            uint64 `gorm:"primaryKey"`
	BranchID      string
	Event         string
	Payload       string
	Status        string
	Attempts      int
	NextAttemptAt time.Time
	LastError     *string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func (WebhookEvent) TableName() string { return "wa_webhook_events" }

type PollVote struct {
	ID              string `gorm:"primaryKey"`
	MessageID       string
	VoterJID        string `gorm:"column:voter_jid"`
	SelectedOptions string
	VotedAt         time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

func (PollVote) TableName() string { return "wa_poll_votes" }

type DeviceEvent struct {
	ID        uint64 `gorm:"primaryKey"`
	DeviceID  string
	Status    string
	Reason    *string
	CreatedAt time.Time
}

func (DeviceEvent) TableName() string { return "wa_device_events" }
