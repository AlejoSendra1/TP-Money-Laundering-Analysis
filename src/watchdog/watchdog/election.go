package watchdog_worker

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"tp_distribuidos/common/messageprotocol/inner"
	"tp_distribuidos/common/middleware"
)

const (
	electionExchangeName = "watchdog_election"
	electionRoutingKey   = "election"

	// leaderHeartbeatInterval is how often the leader broadcasts COORDINATOR.
	leaderHeartbeatInterval = 1 * time.Second

	leaderHeartbeatCheck = leaderHeartbeatInterval / 2

	// leaderTimeout is how long a follower waits without hearing from the leader
	// before triggering a new election.
	leaderTimeout = 3 * time.Second

	// electionWindow is how long to collect ELECTION messages before deciding.
	electionWindow = 1 * time.Second

	// unknownRetriggerDelay is the minimum time between election re-triggers when
	// in stateUnknown (waiting for a higher node's COORDINATOR).
	// Must be > electionWindow to avoid re-triggering before the winner announces.
	unknownRetriggerDelay = electionWindow + 200*time.Millisecond
)

type electionState int

const (
	stateUnknown  electionState = iota // no leader known, election needed
	stateElection                      // election in progress, collecting candidates
	stateFollower                      // following a known leader
	stateLeader                        // this node is the leader
)

// LeaderElection implements a simplified Bully algorithm.
//
// On startup all nodes trigger an election. Once elected, the leader periodically
// broadcasts COORDINATOR heartbeats. If a follower stops hearing from the leader
// for leaderTimeout, it triggers a new election. A higher-ID node always wins.
type LeaderElection struct {
	myID              int
	exchange          middleware.Middleware
	mu                sync.Mutex
	state             electionState
	highestSeen       int       // highest candidate ID seen in current election window
	lastLeaderHeard   time.Time // last time we received a COORDINATOR from the leader
	lastElectionStart time.Time // when the last election round was started
	stopCh            chan struct{}
}

func NewLeaderElection(myID int, connSettings middleware.ConnSettings) (*LeaderElection, error) {
	queueName := fmt.Sprintf("election_queue_%d", myID)
	exchange, err := middleware.CreateExchangeMiddleware(
		electionExchangeName,
		[]string{electionRoutingKey},
		connSettings,
		queueName,
	)
	if err != nil {
		return nil, fmt.Errorf("creating election exchange: %w", err)
	}

	return &LeaderElection{
		myID:     myID,
		exchange: exchange,
		state:    stateUnknown,
		stopCh:   make(chan struct{}),
	}, nil
}

// IsLeader returns whether this instance is currently the elected leader.
func (le *LeaderElection) IsLeader() bool {
	le.mu.Lock()
	defer le.mu.Unlock()
	return le.state == stateLeader
}

// Run starts the election loop and the message consumer. Non-blocking.
func (le *LeaderElection) Run() {
	go le.exchange.StartConsuming(le.handleMessage)
	go le.mainLoop()
}

// Stop shuts down the election module.
func (le *LeaderElection) Stop() {
	close(le.stopCh)
	le.exchange.StopConsuming()
	le.exchange.Close()
}

func (le *LeaderElection) mainLoop() {
	// Leader sends COORDINATOR heartbeats at this rate.
	leaderTicker := time.NewTicker(leaderHeartbeatInterval)
	defer leaderTicker.Stop()

	// Followers check whether the leader is still alive at this rate.
	checkTicker := time.NewTicker(leaderHeartbeatCheck)
	defer checkTicker.Stop()

	// Fires after electionWindow to decide the election result.
	electionTimer := time.NewTimer(electionWindow)
	electionTimer.Stop() // inactive until an election starts
	defer electionTimer.Stop()

	// No leader known on startup — start election immediately.
	le.startElectionRound(electionTimer)

	for {
		select {
		case <-le.stopCh:
			return

		case <-leaderTicker.C:
			le.mu.Lock()
			isLeader := le.state == stateLeader
			le.mu.Unlock()
			if isLeader {
				le.broadcastCoordinator()
			}

		case <-checkTicker.C:
			le.mu.Lock()
			state := le.state
			lastHeard := le.lastLeaderHeard
			lastElection := le.lastElectionStart
			le.mu.Unlock()

			leaderLost := state == stateFollower && time.Since(lastHeard) > leaderTimeout
			// Only re-trigger from stateUnknown after enough time for the winner to
			// send its COORDINATOR (avoids spamming elections every 500ms).
			noLeader := state == stateUnknown && time.Since(lastElection) > unknownRetriggerDelay

			if leaderLost || noLeader {
				slog.Info("election: leader lost or unknown, triggering election", "id", le.myID)
				le.startElectionRound(electionTimer)
			}

		case <-electionTimer.C:
			le.decide()
		}
	}
}

// startElectionRound begins a new election if one is not already in progress.
func (le *LeaderElection) startElectionRound(electionTimer *time.Timer) {
	le.mu.Lock()
	if le.state == stateElection {
		le.mu.Unlock()
		return
	}
	le.state = stateElection
	le.highestSeen = le.myID
	le.lastElectionStart = time.Now()
	le.mu.Unlock()

	le.broadcastElection()

	// Reset the decision timer (drain first if it already fired).
	if !electionTimer.Stop() {
		select {
		case <-electionTimer.C:
		default:
		}
	}
	electionTimer.Reset(electionWindow)
}

// decide is called after electionWindow expires.
// If this node saw no higher candidate, it declares itself leader.
func (le *LeaderElection) decide() {
	le.mu.Lock()
	defer le.mu.Unlock()

	if le.state != stateElection {
		return // a COORDINATOR arrived while we were waiting — already resolved
	}

	if le.highestSeen == le.myID {
		slog.Info("election: I am the new leader", "id", le.myID)
		le.state = stateLeader
		le.broadcastCoordinatorLocked()
	} else {
		// A higher node is alive. It will send COORDINATOR shortly.
		// Transition to unknown so checkTicker re-triggers if it doesn't.
		slog.Info("election: higher node exists, waiting for its COORDINATOR",
			"id", le.myID, "highest", le.highestSeen)
		le.state = stateUnknown
	}
}

func (le *LeaderElection) broadcastElection() {
	msg, err := inner.SerializeWatchdogElection(le.myID)
	if err != nil {
		slog.Error("election: failed to serialize ELECTION", "err", err)
		return
	}
	if err := le.exchange.Send(*msg); err != nil {
		slog.Warn("election: failed to broadcast ELECTION", "err", err)
	}
}

func (le *LeaderElection) broadcastCoordinator() {
	msg, err := inner.SerializeWatchdogCoordinator(le.myID)
	if err != nil {
		slog.Error("election: failed to serialize COORDINATOR", "err", err)
		return
	}
	if err := le.exchange.Send(*msg); err != nil {
		slog.Warn("election: failed to broadcast COORDINATOR", "err", err)
	}
}

// broadcastCoordinatorLocked must be called with le.mu held.
// Temporarily releases the lock to avoid deadlock during Send.
func (le *LeaderElection) broadcastCoordinatorLocked() {
	le.mu.Unlock()
	le.broadcastCoordinator()
	le.mu.Lock()
}

// handleMessage processes incoming ELECTION and COORDINATOR messages.
func (le *LeaderElection) handleMessage(msg middleware.Message, ack, nack func()) {
	msgClient, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("election: failed to deserialize message", "err", err)
		nack()
		return
	}

	senderID, err := inner.DeserializeWatchdogID(msgClient.Data)
	if err != nil {
		slog.Error("election: failed to deserialize watchdog ID", "err", err)
		nack()
		return
	}

	switch msgClient.MsgType {
	case inner.WatchdogElection:
		le.mu.Lock()
		if senderID > le.highestSeen {
			le.highestSeen = senderID
		}
		// Pure Bully: a higher-ID node always takes over, even from an active leader.
		if senderID > le.myID && le.state == stateLeader {
			slog.Info("election: higher node joined, stepping down",
				"my_id", le.myID, "higher_id", senderID)
			le.state = stateUnknown
		}
		isLeader := le.state == stateLeader
		alreadyCandidate := le.state == stateElection
		le.mu.Unlock()

		if isLeader {
			// A lower-ID node is holding an election. Respond immediately with
			// COORDINATOR so it receives our candidacy within its election window,
			// preventing a temporary split-brain.
			le.broadcastCoordinator()
		} else if senderID < le.myID && !alreadyCandidate {
			// We have a higher ID but are not the leader (follower or unknown).
			// Assert our candidacy so the lower-ID node doesn't win by default.
			slog.Debug("election: asserting higher candidacy against lower-ID election",
				"my_id", le.myID, "sender_id", senderID)
			le.broadcastElection()
		}

	case inner.WatchdogCoordinator:
		if senderID == le.myID {
			ack()
			return
		}
		le.mu.Lock()
		le.state = stateFollower
		le.lastLeaderHeard = time.Now()
		le.mu.Unlock()
		slog.Info("election: following coordinator", "leader_id", senderID)
	}

	ack()
}
