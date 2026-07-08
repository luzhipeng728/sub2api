package service

import (
	"testing"
)

func TestOpenAIWSConnPoolSnapshotMetricsReportsActiveConnCount(t *testing.T) {
	pool := newOpenAIWSConnPool(nil)
	defer pool.Close()

	ap1 := pool.getOrCreateAccountPool(101)
	ap1.mu.Lock()
	ap1.conns["c1"] = newOpenAIWSConn("c1", 101, nil, nil)
	ap1.conns["c2"] = newOpenAIWSConn("c2", 101, nil, nil)
	ap1.mu.Unlock()

	ap2 := pool.getOrCreateAccountPool(202)
	ap2.mu.Lock()
	ap2.conns["c3"] = newOpenAIWSConn("c3", 202, nil, nil)
	ap2.mu.Unlock()

	snap := pool.SnapshotMetrics()
	if snap.ActiveConnCount != 3 {
		t.Fatalf("ActiveConnCount = %d, want 3", snap.ActiveConnCount)
	}

	// Removing one connection from the map should be reflected immediately
	// (no separate increment/decrement bookkeeping to drift).
	ap1.mu.Lock()
	delete(ap1.conns, "c1")
	ap1.mu.Unlock()

	snap = pool.SnapshotMetrics()
	if snap.ActiveConnCount != 2 {
		t.Fatalf("ActiveConnCount after delete = %d, want 2", snap.ActiveConnCount)
	}
}
