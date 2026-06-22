package heatbeat

import (
	"fmt"
	"log/slog"
	"time"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
)

// HeartbeatSender periodically sends a heartbeat to the heatbeat exchange,
// identifying itself with its workerID (docker-compose service name).
//
// Typical usage in Run():
//
//	hb, err := heatbeat.NewHeartbeatSender("counter_q2_0", connSettings)
//	hb.Start()
//	// ... on SIGTERM:
//	hb.Stop()
type HeartbeatSender struct {
	workerID string
	output   middleware.Middleware
	stopCh   chan struct{}
}

// NewHeartbeatSender creates a HeartbeatSender and establishes the connection to the exchange.
// workerID must match the service name in docker-compose (e.g. "counter_q2_0").
func NewHeartbeatSender(workerID string, connSettings middleware.ConnSettings) (*HeartbeatSender, error) {
	output, err := middleware.CreateExchangeMiddleware(HeartbeatExchange, []string{workerID}, connSettings, "")
	if err != nil {
		return nil, fmt.Errorf("heartbeat: creating exchange: %w", err)
	}
	return &HeartbeatSender{
		workerID: workerID,
		output:   output,
		stopCh:   make(chan struct{}),
	}, nil
}

// Start launches the non-blocking goroutine that sends heartbeats every HeartbeatInterval.
func (h *HeartbeatSender) Start() {
	go h.heartbeatLoop()
}

func (h *HeartbeatSender) heartbeatLoop() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.stopCh:
			return
		case <-ticker.C:
			h.sendHeartbeat()
		}
	}
}

func (h *HeartbeatSender) sendHeartbeat() {
	msg, err := inner.SerializeHeartbeat(h.workerID)
	if err != nil {
		slog.Error("heartbeat: serialization failed", "worker_id", h.workerID, "err", err)
		return
	}
	if err := h.output.Send(*msg); err != nil {
		slog.Warn("heartbeat: send failed", "worker_id", h.workerID, "err", err)
	}
}

// Stop stops sending heartbeats and closes the exchange.
func (h *HeartbeatSender) Stop() {
	close(h.stopCh)
	h.output.Close()
}
