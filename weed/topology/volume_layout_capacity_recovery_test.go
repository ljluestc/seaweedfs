package topology

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// A volume removed by the heartbeat capacity path (not RecordAssign) must
// still recover once it shrinks — only works if SetVolumeCapacityFull stamps
// fullSince.
func TestSetVolumeCapacityFullStampsFullSinceAndRecovers(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":4000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	topo, vl := setupPickTest(t, layout, 10000)

	initialActive := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount
	initialWritables, _ := vl.GetWritableVolumeCount()
	if initialWritables != 1 {
		t.Fatalf("expected 1 writable volume initially, got %d", initialWritables)
	}

	if !vl.SetVolumeCapacityFull(1) {
		t.Fatalf("SetVolumeCapacityFull should report the volume was writable")
	}
	vl.AdjustActiveVolumeCountForFull(1)
	if w, _ := vl.GetWritableVolumeCount(); w != 0 {
		t.Fatalf("expected 0 writable after capacity full, got %d", w)
	}
	if vl.sizeTracking[1].fullSince.IsZero() {
		t.Fatalf("expected SetVolumeCapacityFull to stamp fullSince")
	}

	// No recovery before capacityRecoveryDelay.
	advanceSizeTrackingClock(vl, 1, 3*time.Second)
	if vl.UpdateVolumeSize(1, 4000, 0) {
		t.Fatalf("recovery should not fire before capacityRecoveryDelay")
	}

	// After the delay, a smaller size restores it.
	advanceSizeTrackingClock(vl, 1, capacityRecoveryDelay)
	if !vl.UpdateVolumeSize(1, 4000, 0) {
		t.Fatalf("expected volume to recover to writable after shrinking")
	}
	vl.AdjustActiveVolumeCountAfterRecovery(1)

	if w, _ := vl.GetWritableVolumeCount(); w != initialWritables {
		t.Fatalf("expected %d writable after recovery, got %d", initialWritables, w)
	}
	if got := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount; got != initialActive {
		t.Fatalf("expected activeVolumeCount restored to %d, got %d", initialActive, got)
	}
	if !vl.sizeTracking[1].fullSince.IsZero() {
		t.Fatalf("expected fullSince cleared after recovery")
	}
}

// TestUpdateVolumeSizeCompactionRecoveryFromVacuum covers the primary scenario
// of issue #3265: a volume removed by DrainAndRemoveFromWritable (vacuum)
// with fullSince NOT set is permanently stuck out of the writable pool because
// the UpdateVolumeSize recovery path requires fullSince to be non-zero.
// After a compaction (compact_revision change), the volume must be re-added
// to writables immediately without waiting for capacityRecoveryDelay, and
// because there was no capacity-full removal (fullSince == zero) the
// activeVolumeCount must NOT be adjusted.
func TestUpdateVolumeSizeCompactionRecoveryFromVacuum(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":4000, "replication":"000"},
          {"id":2, "size":4000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	topo, vl := setupPickTest(t, layout, 10000)
	initialActive := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount
	initialWritables, _ := vl.GetWritableVolumeCount()

	// Simulate vacuum: remove from writables WITHOUT setting fullSince.
	vl.accessLock.Lock()
	vl.removeFromWritable(1) // mimics DrainAndRemoveFromWritable
	vl.accessLock.Unlock()

	if w, _ := vl.GetWritableVolumeCount(); w != initialWritables-1 {
		t.Fatalf("expected %d writables after vacuum drain, got %d", initialWritables-1, w)
	}
	if !vl.sizeTracking[1].fullSince.IsZero() {
		t.Fatalf("DrainAndRemoveFromWritable must not set fullSince")
	}

	// Normal heartbeat (same compactRevision) must NOT recover the volume
	// because fullSince is zero and no compaction has been detected yet.
	advanceSizeTrackingClock(vl, 1, 60*time.Second)
	if vl.UpdateVolumeSize(1, 4000, 0) {
		t.Fatal("UpdateVolumeSize must not return recoveredToWritable when compaction not detected")
	}
	if w, _ := vl.GetWritableVolumeCount(); w != initialWritables-1 {
		t.Errorf("volume should still be non-writable (fullSince path not triggered), got %d writables", w)
	}

	// Heartbeat with a new compactRevision (compaction completed on the volume
	// server, size dropped to 8 bytes — e.g., TTL-expiry compaction or a
	// successful vacuum after a master restart). Must restore writability
	// immediately without waiting for capacityRecoveryDelay.
	advanceSizeTrackingClock(vl, 1, 5*time.Second) // well within any delay
	if vl.UpdateVolumeSize(1, 8, 1) {
		// returning true would mean activeVolumeCount was adjusted, which is
		// wrong here because fullSince was zero (no paired decrement).
		t.Fatal("UpdateVolumeSize must return false when fullSince was zero (no paired activeVolumeCount decrement)")
	}
	if w, _ := vl.GetWritableVolumeCount(); w != initialWritables {
		t.Errorf("expected %d writables after compaction recovery, got %d", initialWritables, w)
	}
	// activeVolumeCount must not change — no paired decrement existed.
	if got := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount; got != initialActive {
		t.Errorf("activeVolumeCount must not change: expected %d, got %d", initialActive, got)
	}
	// fullSince should still be zero.
	if !vl.sizeTracking[1].fullSince.IsZero() {
		t.Error("fullSince should remain zero after compaction-only recovery")
	}
}

// TestUpdateVolumeSizeCompactionRecoveryFromCapacityFull covers the scenario
// where a volume was full (fullSince set, activeVolumeCount decremented) and
// then compacted to a small size. The compaction heartbeat must re-add it to
// writables immediately AND return true so the caller restores activeVolumeCount.
func TestUpdateVolumeSizeCompactionRecoveryFromCapacityFull(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":9500, "replication":"000"},
          {"id":2, "size":4000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	topo, vl := setupPickTest(t, layout, 10000)
	initialActive := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount

	// Mark volume 1 as capacity-full (sets fullSince, decrements activeVolumeCount).
	if !vl.SetVolumeCapacityFull(1) {
		t.Fatal("SetVolumeCapacityFull should return true")
	}
	vl.AdjustActiveVolumeCountForFull(1)
	if got := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount; got != initialActive-1 {
		t.Fatalf("expected activeVolumeCount %d after full, got %d", initialActive-1, got)
	}

	// Compaction heartbeat: size drops to 8, compactRevision increments.
	// Recovery should be immediate (no delay), and must return true so the
	// caller can restore the activeVolumeCount.
	advanceSizeTrackingClock(vl, 1, 5*time.Second)
	if !vl.UpdateVolumeSize(1, 8, 1) {
		t.Fatal("expected UpdateVolumeSize to return true (needs activeVolumeCount restore)")
	}
	vl.AdjustActiveVolumeCountAfterRecovery(1)

	if w, _ := vl.GetWritableVolumeCount(); w != 2 {
		t.Errorf("expected 2 writables after compaction recovery, got %d", w)
	}
	if got := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount; got != initialActive {
		t.Errorf("expected activeVolumeCount restored to %d, got %d", initialActive, got)
	}
	if !vl.sizeTracking[1].fullSince.IsZero() {
		t.Error("fullSince must be cleared after compaction recovery")
	}
}

// TestUpdateVolumeSizeCompactionNoRecoveryWhenStillOversized verifies that
// a compaction event does not recover a volume that is still over the size
// limit after compaction (e.g. a very large volume that compacted but is
// still above the limit).
func TestUpdateVolumeSizeCompactionNoRecoveryWhenStillOversized(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":9500, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	vl.SetVolumeCapacityFull(1)
	vl.AdjustActiveVolumeCountForFull(1)

	// Compaction that still leaves the volume over the limit.
	advanceSizeTrackingClock(vl, 1, 5*time.Second)
	if vl.UpdateVolumeSize(1, 10500, 1) {
		t.Fatal("compaction must not recover a volume still over the size limit")
	}
	if w, _ := vl.GetWritableVolumeCount(); w != 0 {
		t.Errorf("expected 0 writables (still oversized), got %d", w)
	}
}

func TestSetVolumeAvailableRestoresActiveCountForCapacityFullVolume(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":4000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	topo, vl := setupPickTest(t, layout, 10000)

	initialActive := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount
	if !vl.SetVolumeCapacityFull(1) {
		t.Fatalf("SetVolumeCapacityFull should report the volume was writable")
	}
	vl.AdjustActiveVolumeCountForFull(1)
	if got := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount; got != initialActive-1 {
		t.Fatalf("expected activeVolumeCount decremented to %d, got %d", initialActive-1, got)
	}

	vl.accessLock.RLock()
	dn := vl.vid2location[1].list[0]
	vl.accessLock.RUnlock()

	if !vl.SetVolumeAvailable(dn, 1, false, false) {
		t.Fatalf("SetVolumeAvailable should report the volume became writable")
	}
	if got := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount; got != initialActive {
		t.Fatalf("expected SetVolumeAvailable to restore activeVolumeCount to %d, got %d", initialActive, got)
	}
	if !vl.sizeTracking[1].fullSince.IsZero() {
		t.Fatalf("expected SetVolumeAvailable to clear fullSince after restoring activeVolumeCount")
	}

	advanceSizeTrackingClock(vl, 1, capacityRecoveryDelay+time.Second)
	if vl.UpdateVolumeSize(1, 4000, 0) {
		t.Fatalf("heartbeat recovery should not fire after SetVolumeAvailable already restored the volume")
	}
	if got := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount; got != initialActive {
		t.Fatalf("expected activeVolumeCount to remain %d, got %d", initialActive, got)
	}
}
