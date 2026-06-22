package heatbeat

import "time"

const (
	// HeartbeatInterval is the time between heartbeat sends from an observed worker.
	HeartbeatInterval = 500 * time.Millisecond

	// HeartbeatTimeout is how long the heatbeat waits without receiving a heartbeat
	// before considering a worker dead.
	HeartbeatTimeout = 4 * HeartbeatInterval

	// HeartbeatExchange is the exchange name where all workers publish their heartbeats
	// and from which the heatbeat consumes.
	HeartbeatExchange = "watchdog_heartbeats"
)
