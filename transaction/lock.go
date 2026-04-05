package transaction

import (
	"fmt"
	"sync"

	"mit.edu/dsg/godb/common"
)

// DBLockTag identifies a unique resource (Table or Tuple). It represents Tuple if it has a full RecordID, and
// represents a table if only the oid is set and the rest are set to -1
type DBLockTag struct {
	common.RecordID
}

// NewTableLockTag creates a DBLockTag representing a whole table.
func NewTableLockTag(oid common.ObjectID) DBLockTag {
	return DBLockTag{
		RecordID: common.RecordID{
			PageID: common.PageID{
				Oid:     oid,
				PageNum: -1,
			},
			Slot: -1,
		},
	}
}

// NewTupleLockTag creates a DBLockTag representing a specific tuple (row).
func NewTupleLockTag(rid common.RecordID) DBLockTag {
	return DBLockTag{
		RecordID: rid,
	}
}

func (t DBLockTag) String() string {
	if t.PageNum == -1 {
		return fmt.Sprintf("Table(%d)", t.Oid)
	}
	return fmt.Sprintf("Tuple(%d, %d, %d)", t.Oid, t.PageNum, t.Slot)
}

// DBLockMode represents the type of access a transaction is requesting.
// GoDB supports a standard Multi-Granularity Locking hierarchy.
type DBLockMode int

const (
	// LockModeS (Shared) allows reading a resource. Multiple transactions can hold S locks simultaneously.
	LockModeS DBLockMode = iota
	// LockModeX (Exclusive) allows modification. It is incompatible with all other modes.
	LockModeX
	// LockModeIS (Intent Shared) indicates the intention to read resources at a lower level (e.g., locking a table IS to read tuples).
	LockModeIS
	// LockModeIX (Intent Exclusive) indicates the intention to modify resources at a lower level (e.g., locking a table IX to modify tuples).
	LockModeIX
	// LockModeSIX (Shared Intent Exclusive) allows reading the resource (like S) AND the intention to modify lower-level resources (like IX).
	LockModeSIX
)

func (m DBLockMode) String() string {
	switch m {
	case LockModeS:
		return "LockModeS"
	case LockModeX:
		return "LockModeX"
	case LockModeIS:
		return "LockModeIS"
	case LockModeIX:
		return "LockModeIX"
	case LockModeSIX:
		return "LockModeSIX"
	}
	return "Unknown lock mode"
}

// compatibility matrix (from lecture 12)
var compatible = [5][5]bool{
	//          S      X      IS     IX     SIX
	/* S   */ { true,  false, true,  false, false},
	/* X   */ { false, false, false, false, false},
	/* IS  */ { true,  false, true,  true,  true },
	/* IX  */ { false, false, true,  true,  false},
	/* SIX */ { false, false, true,  false, false},
}

var mergeMode = [5][5]DBLockMode{
	//            S          X          IS         IX         SIX
	/* S   */ {LockModeS,  LockModeX, LockModeS, LockModeSIX, LockModeSIX},
	/* X   */ {LockModeX,  LockModeX, LockModeX, LockModeX,   LockModeX},
	/* IS  */ {LockModeS,  LockModeX, LockModeIS, LockModeIX, LockModeSIX},
	/* IX  */ {LockModeSIX, LockModeX, LockModeIX, LockModeIX, LockModeSIX},
	/* SIX */ {LockModeSIX, LockModeX, LockModeSIX, LockModeSIX, LockModeSIX},
  }

type lockRequest struct {
	tid common.TransactionID
	mode DBLockMode
	granted chan error
}

type lockEntry struct {
	holders map[common.TransactionID]DBLockMode
	waiters []*lockRequest
}

// LockManager manages the granting, releasing, and waiting of locks on database resources.
type LockManager struct {
	// Add fields here
	mu sync.Mutex
	locks map[DBLockTag]*lockEntry
	waitingFor map[common.TransactionID]DBLockTag
}

func canGrant(entry *lockEntry, tid common.TransactionID, requested DBLockMode) bool {
	for holderTid, heldMode := range entry.holders {
		if holderTid == tid {
			continue // skip self for upgrade case
		}
		if !compatible[heldMode][requested] {
			return false
		}
	}
	return true
}

func (lm *LockManager) detectDeadlock(startTid common.TransactionID, startTag DBLockTag, startMode DBLockMode) bool {
	visited := make(map[common.TransactionID]bool)
	// find all incompatible holders
	stack := []common.TransactionID{}
	entry, entryExists := lm.locks[startTag]
	if !entryExists {
		return false
	}
	// Initial seeds: incompatible holders
	for holderTid, heldMode := range entry.holders {
		if holderTid != startTid && !compatible[heldMode][startMode] {
			stack = append(stack, holderTid)
		}
	}
	// Also depend on earlier waiters whose requested mode conflicts
	for _, req := range entry.waiters {
		if req.tid != startTid && !compatible[req.mode][startMode] {
			stack = append(stack, req.tid)
		}
	}
	for len(stack) != 0 {
		// pop from stack
		Tj := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if Tj == startTid {
			return true // cycle detected
		}

		if visited[Tj] {
			continue
		}
		visited[Tj] = true

		// is Tj also waiting on something?
		if waitTag, ok := lm.waitingFor[Tj]; ok {
			entry, tagOk := lm.locks[waitTag]
			if !tagOk {
				delete(lm.waitingFor, Tj)
				continue
			}
			// find Tj's mode
			var requestedMode DBLockMode
			found := false
			for _, req := range entry.waiters {
				if req.tid == Tj {
					requestedMode = req.mode
					found = true
					break
				}
			}
			if !found {
				delete(lm.waitingFor, Tj)
  				continue
			}
			// follow edges, as Tj waits for all incompatible holders
			for Tk, heldMode := range entry.holders {
				if Tk != Tj && !compatible[heldMode][requestedMode] {
					stack = append(stack, Tk)
				}
			}
			for _, w := range entry.waiters {
				if w.tid != Tj && !compatible[w.mode][requestedMode] {
					stack = append(stack, w.tid)
				}
			}
		}
	}
	return false
}

// NewLockManager initializes a new LockManager.
func NewLockManager() *LockManager {
	return &LockManager{
		locks: make(map[DBLockTag]*lockEntry),
		waitingFor: make(map[common.TransactionID]DBLockTag),
	}
}

// Lock acquires a lock on a specific resource (Table or Tuple) with the requested mode. If the lock cannot be acquired
// immediately, the transaction blocks until it is granted or aborted. It returns nil if the lock is successfully
// acquired, or GoDBError(DeadlockError) in case of a (potential or detected) deadlock.
func (lm *LockManager) Lock(tid common.TransactionID, tag DBLockTag, mode DBLockMode) error {
	lm.mu.Lock()

	entry, entryExists := lm.locks[tag]
	if !entryExists {
		entry = &lockEntry{
			holders: make(map[common.TransactionID]DBLockMode),
		}
		lm.locks[tag] = entry
	}
	// check if we can grant the lock immediately
	heldMode, alreadyHeld := entry.holders[tid]
	if alreadyHeld {
		if heldMode == mode {
			lm.mu.Unlock()
			return nil
		}
		if canGrant(entry, tid, mode) {
			entry.holders[tid] = mergeMode[heldMode][mode]
			lm.mu.Unlock()
			return nil
		}
	}
	if canGrant(entry, tid, mode) && len(entry.waiters) == 0 {
	  entry.holders[tid] = mode
	  lm.mu.Unlock()
	  return nil
	}
	// Deadlock detection BEFORE adding to wait queue
	if lm.detectDeadlock(tid, tag, mode) {
		lm.mu.Unlock()
		return common.GoDBError{Code: common.DeadlockError, ErrString: "..."}
	}

	req := &lockRequest{tid: tid, mode: mode, granted: make(chan error, 1)}
	entry.waiters = append(entry.waiters, req)
	lm.waitingFor[tid] = tag

	lm.mu.Unlock()
	return <-req.granted
}

// Unlock releases the lock held by the transaction on the specified resource. If the requesting transaction does not
// hold the specified lock, it should return GoDBError(LockNotFoundError)
func (lm *LockManager) Unlock(tid common.TransactionID, tag DBLockTag) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, exists := lm.locks[tag]
	if !exists {
		return common.GoDBError{Code: common.LockNotFoundError, ErrString: "..."}
	}
	if _, ok := entry.holders[tid]; !ok {
		return common.GoDBError{Code: common.LockNotFoundError, ErrString: "..."}
	}
	delete(entry.holders, tid)

	granted := 0
	for _, req := range entry.waiters {
	if !canGrant(entry, req.tid, req.mode) {
		break
	}
	entry.holders[req.tid] = req.mode
	delete(lm.waitingFor, req.tid)
	select {
	case req.granted <- nil:
	default:
	}
	granted++
	}
	entry.waiters = entry.waiters[granted:]

	if len(entry.holders) == 0 && len(entry.waiters) == 0 {
		delete(lm.locks, tag)
	}

	return nil
}

// LockHeld checks if any transaction currently holds a lock on the given resource.
func (lm *LockManager) LockHeld(tag DBLockTag) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, exists := lm.locks[tag]
	return exists && len(entry.holders) > 0
}
