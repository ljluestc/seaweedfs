package topology

import (
	"sync"

	"github.com/seaweedfs/raft"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// goRaftLeaderCache mirrors the leader name and state published by a
// github.com/seaweedfs/raft ("goraft") server.
//
// The library writes server.leader from its event loop while holding the
// server mutex, but server.Leader() reads that field without taking the lock.
// Every RaftServer.Leader() call made from a master goroutine therefore races
// with an election or a step down, which is the data race reported in
// issue #3509:
//
//	Write at ... raft.(*server).setState() server.go:299
//	Previous read at ... raft.(*server).Leader() server.go:237
//
// The cache is filled from the raft server's own event listeners. Those
// listeners run on the raft goroutine that performs the write, so the value
// is published through this mutex and readers never touch the library's
// unsynchronized fields.
type goRaftLeaderCache struct {
	sync.RWMutex
	name   string // this server's raft name, immutable once attached
	leader string // last leader name announced by the raft event loop
	state  string // last state announced by the raft event loop
}

// AttachGoRaftEventListeners subscribes the leader cache to raftServer.
//
// It must be called before raftServer.Start(), otherwise the events emitted by
// the very first election are dispatched before the listeners exist and the
// cache starts out stale.
func (t *Topology) AttachGoRaftEventListeners(raftServer raft.Server) {
	t.goRaftLeader.Lock()
	t.goRaftLeader.name = raftServer.Name()
	t.goRaftLeader.Unlock()

	raftServer.AddEventListener(raft.LeaderChangeEventType, func(e raft.Event) {
		leader, _ := e.Value().(string)
		t.setGoRaftLeader(leader)
	})
	raftServer.AddEventListener(raft.StateChangeEventType, func(e raft.Event) {
		state, _ := e.Value().(string)
		t.setGoRaftState(state)
	})
}

func (t *Topology) setGoRaftLeader(leader string) {
	t.goRaftLeader.Lock()
	defer t.goRaftLeader.Unlock()
	t.goRaftLeader.leader = leader
}

func (t *Topology) setGoRaftState(state string) {
	t.goRaftLeader.Lock()
	defer t.goRaftLeader.Unlock()
	t.goRaftLeader.state = state
	// A leader that loses quorum clears server.leader inline and then calls
	// setState(Follower), so no leader change event is emitted for that
	// transition. Drop the stale self reference here, otherwise callers keep
	// being pointed at a master that has already stepped down.
	if state != raft.Leader && t.goRaftLeader.name != "" && t.goRaftLeader.leader == t.goRaftLeader.name {
		t.goRaftLeader.leader = ""
	}
}

// GoRaftLeader returns the last leader name announced by the goraft event
// loop, or "" when no leader is currently known. It is the race free
// replacement for RaftServer.Leader().
func (t *Topology) GoRaftLeader() string {
	t.goRaftLeader.RLock()
	defer t.goRaftLeader.RUnlock()
	return t.goRaftLeader.leader
}

// GoRaftState returns the last state announced by the goraft event loop, or
// "" before the raft server has started.
func (t *Topology) GoRaftState() string {
	t.goRaftLeader.RLock()
	defer t.goRaftLeader.RUnlock()
	return t.goRaftLeader.state
}

// goRaftLeaderIsSelf reports whether the cached leader is this master. The
// comparison goes through pb.ServerAddress so that the http and grpc forms of
// the same address compare equal.
func (t *Topology) goRaftLeaderIsSelf() bool {
	t.goRaftLeader.RLock()
	defer t.goRaftLeader.RUnlock()
	if t.goRaftLeader.leader == "" || t.goRaftLeader.name == "" {
		return false
	}
	return pb.ServerAddress(t.goRaftLeader.name).Equals(pb.ServerAddress(t.goRaftLeader.leader))
}
