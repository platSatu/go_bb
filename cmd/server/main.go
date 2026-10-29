package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types/events"

	"go_bizbos/internal/api"
	"go_bizbos/internal/config"
	"go_bizbos/internal/device"
	"go_bizbos/internal/media"
	"go_bizbos/internal/message"
	"go_bizbos/internal/notify"
	"go_bizbos/internal/realtime"
	"go_bizbos/internal/sender"
	"go_bizbos/internal/store"
	"go_bizbos/internal/webhook"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("konfigurasi: %v", err)
	}
	db, err := store.Open(cfg)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	hub := realtime.NewHub()
	hooks := webhook.New(db, cfg.WebhookURL, cfg.WebhookSecret)
	bus := &notify.Bus{Hub: hub, Webhook: hooks}

	devices, err := device.NewManager(ctx, db, cfg.DataDir, bus)
	if err != nil {
		log.Fatal(err)
	}
	client := func(id string) *whatsmeow.Client { return devices.Client(id) }
	inbox := message.NewInbox(db, bus)
	files := media.New(db, cfg.DataDir, bus, client)
	out := sender.New(db, cfg, bus, inbox, files, client)

	devices.OnReady = func(id string) { out.Kick(ctx, id) }
	devices.OnEvent = func(s *device.Session, evt any) {
		switch e := evt.(type) {
		case *events.Message:
			inbox.OnMessage(s.BranchID, s.DeviceID, s.Client, e)
		case *events.Receipt:
			inbox.OnReceipt(s.BranchID, s.DeviceID, s.Client, e)
		case *events.HistorySync:
			// Riwayat chat setelah HP ditautkan; bisa besar, jadi tidak menahan event lain.
			go inbox.OnHistorySync(s.BranchID, s.DeviceID, s.Client, e)
		case *events.MediaRetry:
			files.HandleRetry(s.BranchID, s.DeviceID, e)
		case *events.Presence, *events.ChatPresence:
			inbox.OnPresence(s.BranchID, s.DeviceID, s.Client, e)
		}
	}

	go hooks.Run(ctx)
	go out.Run(ctx)
	go devices.Watch(ctx)
	go devices.Restore(ctx)

	h := &api.Handler{DB: db, Cfg: cfg, Devices: devices, Media: files, Sender: out, Hub: hub}
	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           h.Router(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       5 * time.Minute,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    64 << 10,
		BaseContext:       func(net.Listener) context.Context { return ctx }, // SSE ikut berhenti saat shutdown
	}
	go func() {
		log.Printf("go_bizbos berjalan di %s", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Print("mematikan server...")
	shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	devices.Shutdown()
}
