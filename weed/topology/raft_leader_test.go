package topology

import (
	"sync"
	"testing"

	"github.com/seaweedfs/raft"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
)

// fakeRaftEvent is a minimal raft.Event carrying the new and previous value.
type fakeRaftEvent struct {
	typ   string
	value interface{}
	prev  interface{}
}

func (e fakeRaftEvent) Type() string           { return e.typ }
func (e fakeRaftEvent) Source() interface{}    { return nil }
func (e fakeRaftEvent) Value() interface{}     { return e.value }
func (e fakeRaftEvent) PrevValue() interface{} { return e.prev }

// fakeRaftServer implements just enough of raft.Server for the leader cache:
// a name, listener registration and a state. Every other method comes from the
// embedded nil interface and panics if called, which keeps the test honest
// about what the cache is allowed to touch.
type fakeRaftServer struct {
	raft.Server
	name string

	mu        sync.Mutex
	state     string
	listeners map[string][]raft.EventListener
}

func newFakeRaftServer(name string) *fakeRaftServer {
	return &fakeRaftServer{
		name:      name,
		state:     raft.Follower,
		listeners: make(map[string][]raft.EventListener),
	}
}

func (s *fakeRaftServer) Name() string { return s.name }

func (s *fakeRaftServer) State() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *fakeRaftServer) AddEventListener(typ string, listener raft.EventListener) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listeners[typ] = append(s.listeners[typ], listener)
}

// setState mirrors what the raft event loop does: it flips the state and
// dispatches the state change event to the registered listeners.
func (s *fakeRaftServer) setState(state string) {
	s.mu.Lock()
	s.state = state
	listeners := append([]raft.EventListener(nil), s.listeners[raft.StateChangeEventType]...)
	s.mu.Unlock()
	for _, l := range listeners {
		l(fakeRaftEvent{typ: raft.StateChangeEventType, value: state})
	}
}

func (s *fakeRaftServer) setLeader(leader string) {
	s.mu.Lock()
	listeners := append([]raft.EventListener(nil), s.listeners[raft.LeaderChangeEventType]...)
	s.mu.Unlock()
	for _, l := range listeners {
		l(fakeRaftEvent{typ: raft.LeaderChangeEventType, value: leader})
	}
}

func newTestTopologyWithRaft(name string) (*Topology, *fakeRaftServer) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	rs := newFakeRaftServer(name)
	topo.AttachGoRaftEventListeners(rs)
	topo.RaftServer = rs
	return topo, rs
}

func TestGoRaftLeaderCacheFollowsEvents(t *testing.T) {
	topo, rs := newTestTopologyWithRaft("master1:9333")

	if got := topo.GoRaftLeader(); got != "" {
		t.Fatalf("leader before any event = %q, want empty", got)
	}
	if topo.IsLeader() {
		t.Fatal("IsLeader before any event = true, want false")
	}

	rs.setLeader("master2:9333")
	if got := topo.GoRaftLeader(); got != "master2:9333" {
		t.Fatalf("leader = %q, want master2:9333", got)
	}
	if l, err := topo.MaybeLeader(); err != nil || string(l) != "master2:9333" {
		t.Fatalf("MaybeLeader = %q, %v; want master2:9333, nil", l, err)
	}
	if topo.IsLeader() {
		t.Fatal("IsLeader while another master leads = true, want false")
	}

	rs.setLeader("master1:9333")
	if !topo.IsLeader() {
		t.Fatal("IsLeader after becoming leader = false, want true")
	}
}

// A leader that loses quorum clears its leader field without emitting a leader
// change event, so the cache has to drop the stale self reference when the
// state change arrives.
func TestGoRaftLeaderClearedOnStepDown(t *testing.T) {
	topo, rs := newTestTopologyWithRaft("master1:9333")

	rs.setState(raft.Leader)
	rs.setLeader("master1:9333")
	if !topo.IsLeader() {
		t.Fatal("IsLeader as leader = false, want true")
	}

	rs.setState(raft.Follower)
	if got := topo.GoRaftLeader(); got != "" {
		t.Fatalf("leader after step down = %q, want empty", got)
	}
	if topo.IsLeader() {
		t.Fatal("IsLeader after step down = true, want false")
	}
	if got := topo.GoRaftState(); got != raft.Follower {
		t.Fatalf("state after step down = %q, want %q", got, raft.Follower)
	}
}

// A leader announced by another master survives our own state changes.
func TestGoRaftLeaderKeptForRemoteLeader(t *testing.T) {
	topo, rs := newTestTopologyWithRaft("master1:9333")

	rs.setLeader("master2:9333")
	rs.setState(raft.Candidate)
	if got := topo.GoRaftLeader(); got != "master2:9333" {
		t.Fatalf("leader after local state change = %q, want master2:9333", got)
	}
}

// Leader lookups must stay race free while the raft event loop publishes
// changes, which is what issue #3509 reported.
func TestGoRaftLeaderConcurrentAccess(t *testing.T) {
	topo, rs := newTestTopologyWithRaft("master1:9333")

	const iterations = 500
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			rs.setState(raft.Candidate)
			rs.setLeader("master1:9333")
			rs.setState(raft.Leader)
			rs.setLeader("")
			rs.setState(raft.Follower)
		}
	}()

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				topo.IsLeader()
				topo.MaybeLeader()
				topo.GoRaftLeader()
				topo.GoRaftState()
			}
		}()
	}

	wg.Wait()
}
