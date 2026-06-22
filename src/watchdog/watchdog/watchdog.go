package watchdog_worker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	wd "tp_distribuidos/common/heatbeat"
	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
)

// workerIDFmt is the format for the watchdog's own worker ID sent as heartbeat.
const workerIDFmt = "watchdog_%d"

const (
	dockerSockPath    = "/var/run/docker.sock"
	dockerStartURLFmt = "http://localhost/v1.45/containers/%s/start"
)

type WatchdogConfig struct {
	ID      int
	MomHost string
	MomPort int
}

// Watchdog listens to heartbeats from all workers and restarts those that
// stop sending them after the HeartbeatTimeout.
type Watchdog struct {
	config     WatchdogConfig
	input      middleware.Middleware
	lastSeen   map[string]time.Time
	mu         sync.Mutex
	stopCh     chan struct{}
	httpClient *http.Client
	election   *LeaderElection
	heartbeat  *wd.HeartbeatSender
}

func NewWatchdog(config WatchdogConfig) (*Watchdog, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	// Each instance has its own queue bound to the exchange with "#" (receives all heartbeats)
	queueName := fmt.Sprintf("watchdog_queue_%d", config.ID)
	input, err := middleware.CreateExchangeMiddleware(wd.HeartbeatExchange, []string{"#"}, connSettings, queueName)
	if err != nil {
		return nil, fmt.Errorf("creating heartbeat exchange: %w", err)
	}

	// HTTP client over Docker socket
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", dockerSockPath)
			},
		},
	}

	election, err := NewLeaderElection(config.ID, connSettings)
	if err != nil {
		input.Close()
		return nil, fmt.Errorf("creating leader election: %w", err)
	}

	workerID := fmt.Sprintf(workerIDFmt, config.ID)
	heartbeat, err := wd.NewHeartbeatSender(workerID, connSettings)
	if err != nil {
		input.Close()
		election.Stop()
		return nil, fmt.Errorf("creating heartbeat sender: %w", err)
	}

	return &Watchdog{
		config:     config,
		input:      input,
		lastSeen:   make(map[string]time.Time),
		stopCh:     make(chan struct{}),
		httpClient: httpClient,
		election:   election,
		heartbeat:  heartbeat,
	}, nil
}

// Run starts the watchdog. Blocks until SIGTERM is received.
func (w *Watchdog) Run() {
	w.election.Run()
	w.heartbeat.Start()
	go w.handleSigterm()
	go w.monitorLoop()
	w.input.StartConsuming(w.handleMessage)
	w.input.Close()
}

func (w *Watchdog) handleSigterm() {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM)
	<-signalCh
	slog.Info("watchdog: SIGTERM received, shutting down")
	w.heartbeat.Stop()
	w.election.Stop()
	w.input.StopConsuming()
	close(w.stopCh)
}

// handleMessage updates the last heartbeat timestamp for the worker.
func (w *Watchdog) handleMessage(msg middleware.Message, ack, nack func()) {
	msgClient, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("heatbeat: failed to deserialize message", "err", err)
		nack()
		return
	}

	workerID, err := inner.DeserializeHeartbeat(msgClient.Data)
	if err != nil {
		slog.Error("heatbeat: failed to deserialize heartbeat", "err", err)
		nack()
		return
	}

	w.updateLastSeen(workerID)

	slog.Debug("heatbeat: heartbeat received", "worker_id", workerID)
	ack()
}

func (w *Watchdog) updateLastSeen(workerID string) {
	w.mu.Lock()
	w.lastSeen[workerID] = time.Now()
	w.mu.Unlock()
}

// monitorLoop periodically checks whether any worker has stopped sending heartbeats.
func (w *Watchdog) monitorLoop() {
	ticker := time.NewTicker(wd.HeartbeatTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.checkWorkers()
		}
	}
}

// checkWorkers detects dead workers and restarts them if this instance is the leader.
func (w *Watchdog) checkWorkers() {
	dead := w.collectDeadWorkers()
	if w.election.IsLeader() {
		w.restartDeadWorkers(dead)
	}
}

// collectDeadWorkers returns the IDs of workers that exceeded the HeartbeatTimeout
// and resets their lastSeen to prevent repeated restarts while the container is starting up.
func (w *Watchdog) collectDeadWorkers() []string {
	now := time.Now()

	w.mu.Lock()
	defer w.mu.Unlock()

	var dead []string
	for workerID, lastHeartbeat := range w.lastSeen {
		if now.Sub(lastHeartbeat) > wd.HeartbeatTimeout {
			dead = append(dead, workerID)
			w.lastSeen[workerID] = now
		}
	}
	return dead
}

func (w *Watchdog) restartDeadWorkers(dead []string) {
	for _, workerID := range dead {
		slog.Info("heatbeat: worker is dead, restarting", "worker_id", workerID)
		if err := w.restartWorker(workerID); err != nil {
			slog.Error("heatbeat: failed to restart worker", "worker_id", workerID, "err", err)
		}
	}
}

// restartWorker calls the Docker HTTP API via Unix socket to start the container.
func (w *Watchdog) restartWorker(workerID string) error {
	url := fmt.Sprintf(dockerStartURLFmt, workerID)

	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("docker API call failed: %w", err)
	}
	defer resp.Body.Close()

	// 204: started successfully, 304: already running
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotModified {
		body := make([]byte, 512)
		n, _ := resp.Body.Read(body)
		return fmt.Errorf("unexpected docker API status: %d — %s", resp.StatusCode, string(body[:n]))
	}

	slog.Info("watchdog: worker restarted successfully", "worker_id", workerID)
	return nil
}
