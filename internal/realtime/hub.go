package realtime

import (
	"encoding/json"
	"errors"
	"sync"
)

const (
	perBranchLimit = 20
	bufferSize     = 64
)

type Event struct {
	Name string
	Data []byte
}

type subscriber struct {
	ch     chan Event
	device string
}

// Hub mengirim event hanya ke pelanggan di branch yang sama.
type Hub struct {
	mu   sync.RWMutex
	subs map[string]map[*subscriber]struct{}
}

func NewHub() *Hub { return &Hub{subs: map[string]map[*subscriber]struct{}{}} }

var ErrTooMany = errors.New("terlalu banyak koneksi realtime")

// Subscribe: device kosong berarti semua device di branch.
func (h *Hub) Subscribe(branch, device string) (<-chan Event, func(), error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.subs[branch]) >= perBranchLimit {
		return nil, nil, ErrTooMany
	}
	s := &subscriber{ch: make(chan Event, bufferSize), device: device}
	if h.subs[branch] == nil {
		h.subs[branch] = map[*subscriber]struct{}{}
	}
	h.subs[branch][s] = struct{}{}

	var once sync.Once
	return s.ch, func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.subs[branch], s)
			if len(h.subs[branch]) == 0 {
				delete(h.subs, branch)
			}
			h.mu.Unlock()
		})
	}, nil
}

func (h *Hub) Publish(branch, device, name string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.subs[branch] {
		if s.device != "" && s.device != device {
			continue
		}
		select {
		case s.ch <- Event{Name: name, Data: data}:
		default: // pelanggan lambat: event dilewati, browser tetap bisa memuat ulang dari bizbos
		}
	}
}
