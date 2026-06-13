// fault_injector.go (solo para testing, con build tag)
package datasaver

import (
	"fmt"
	"log/slog"
	"os"
)

type CrashPoint string

const (
	CrashAfterLog        CrashPoint = "CRASH_AFTER_LOG"
	CrashAfterCheckpoint CrashPoint = "CRASH_AFTER_CHECKPOINT"
	CrashAfterSendData   CrashPoint = "CRASH_AFTER_SEND_DATA"
	CrashBeforeEOF       CrashPoint = "CRASH_BEFORE_EOF"
	CrashAfterRename     CrashPoint = "CRASH_AFTER_RENAME" // dentro de writeFile
)

var crashCounters = map[CrashPoint]int{}

// Crash crashea en el punto indicado luego de N invocaciones
func Crash(point CrashPoint) {
	envKey := string(point)
	val := os.Getenv(envKey)
	if val == "" {
		return
	}

	crashCounters[point]++
	threshold := 0
	fmt.Sscanf(val, "%d", &threshold)

	if crashCounters[point] >= threshold {
		slog.Warn("FAULT INJECTION: crashing", "point", point, "count", crashCounters[point])
		os.Exit(1)
	}
}
