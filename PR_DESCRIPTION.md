# master: fix DATA RACE on the goraft leader field (#3509)

## Problem

Starting a master with `-race` reports a data race as soon as the first
election happens ("Initializing new cluster"):

```
WARNING: DATA RACE
Write at 0x00c000700078 by goroutine 56:
  github.com/seaweedfs/raft.(*server).setState()      server.go:299
  github.com/seaweedfs/raft.(*server).followerLoop()  server.go:701

Previous read at 0x00c000700078 by goroutine 48:
  github.com/seaweedfs/raft.(*server).Leader()        server.go:237
  topology.(*Topology).MaybeLeader()                  topology.go:118
  topology.(*Topology).Leader()                       topology.go:99
  topology.(*Topology).IsLeader()                     topology.go:84
  topology.(*Topology).StartRefreshWritableVolumes.func1()
```

`github.com/seaweedfs/raft` writes `server.leader` from its event loop while
holding the server mutex, but reads it back without any lock:

```go
// server.go:236
func (s *server) Leader() string {
	return s.leader
}
```

Every `RaftServer.Leader()` call made from a master goroutine — the volume
refresh loop, the vacuum loop, HTTP handlers, gRPC handlers, the bootstrap
check — therefore races with an election or a step down. The existing
`Topology.RaftServerAccessLock` does not help: it guards the `Topology.RaftServer`
*field*, not the raft server's internal state, and the writer lives inside the
library where seaweedfs cannot take that lock.

Besides the sanitizer warning, the read is genuinely unsynchronized: a master
can observe a torn or stale leader name and proxy a request to a master that
has already stepped down.

## Fix

Stop reading the library's unsynchronized field. `Topology` now keeps a small
cache of the leader name and state, fed by the raft server's own event
listeners:

* New `weed/topology/raft_leader.go` holds `goRaftLeaderCache`
  (`sync.RWMutex` + `name`/`leader`/`state`) plus `AttachGoRaftEventListeners`,
  `GoRaftLeader`, `GoRaftState` and `goRaftLeaderIsSelf`.
* The listeners are registered in `NewRaftServer` **before**
  `raftServer.Start()`, so the events emitted by the very first election are
  not missed.
* Listeners run on the same raft goroutine that performs the write, so the
  value is published through the cache mutex and readers never touch the
  library's fields.

Every leader read in the master now goes through the cache:

| Call site | Before | After |
| --- | --- | --- |
| `topology.IsLeader` | `RaftServer.Leader()` + `Name()` compare | `goRaftLeaderIsSelf()` |
| `topology.MaybeLeader` | `RaftServer.Leader()` | `GoRaftLeader()` |
| `server.NewRaftServer` (fast resume, startup log) | `raftServer.Leader()` | `topo.GoRaftLeader()` |
| `server.SetRaftServer` leader-change listener | `RaftServer.Leader()`/`Name()` | the event's own `Value()` |
| `server.SetRaftServer` follower log line | `RaftServer.Leader()` | `GoRaftLeader()` |
| `RaftListClusterServers` | `RaftServer.Leader()` | `GoRaftLeader()` |
| `command/master.go` bootstrap check | `RaftServer.Leader()` | `GoRaftLeader()` |
| master status UI template | `{{ .RaftServer.Leader }}` | `{{ $.RaftLeader }}`, passed in by the handler |

`RaftServer.State()`, `Peers()` and `IsLogEmpty()` are left alone — those do
take the library's lock. `RaftServer.Name()` is written once at construction
and never mutated, so it is not part of the race.

### One subtlety: step down without an event

A leader that loses quorum clears `server.leader` inline and only then calls
`setState(Follower)`, so no `LeaderChangeEvent` is emitted for that transition
(`server.go` leaderLoop). The state-change listener therefore drops a stale
self-reference:

```go
if state != raft.Leader && t.goRaftLeader.leader == t.goRaftLeader.name {
	t.goRaftLeader.leader = ""
}
```

Without this the cache would keep pointing callers at a master that has
already stepped down. A leader announced by *another* master is left untouched
by our own state changes.

## Tests

New `weed/topology/raft_leader_test.go`, driven by a fake raft server that
dispatches the same events the library does:

* `TestGoRaftLeaderCacheFollowsEvents` — leader change events drive
  `GoRaftLeader`, `MaybeLeader` and `IsLeader`.
* `TestGoRaftLeaderClearedOnStepDown` — the quorum-loss path clears the cached
  self-leader.
* `TestGoRaftLeaderKeptForRemoteLeader` — a remote leader survives local state
  changes.
* `TestGoRaftLeaderConcurrentAccess` — readers hammer `IsLeader`/`MaybeLeader`
  while the event loop publishes changes.

```
go test -race -run GoRaft ./weed/topology/   ok
go test ./weed/topology/... ./weed/server/... ok
go vet ./weed/topology/... ./weed/server/... ./weed/command/...  clean
```

## Compatibility

No behaviour change for hashicorp raft (`HashicorpRaft.Leader()` is already
race free), no configuration change, no on-disk format change, and no change
to the raft dependency.

Fixes #3509
