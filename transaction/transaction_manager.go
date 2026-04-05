package transaction

import (
	"sync"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v3"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/storage"
)

// activeTxnEntry tracks a running transaction and its starting point in the log.
type activeTxnEntry struct {
	txn      *TransactionContext
	startLsn storage.LSN
}

// TransactionManager is the central component managing the lifecycle of transactions.
// It coordinates with the LockManager for concurrency control and the LogManager for
// Write-Ahead Logging (WAL) and recovery.
type TransactionManager struct {
	// activeTxns maps TransactionIDs to their runtime context and metadata
	activeTxns *xsync.MapOf[common.TransactionID, activeTxnEntry]

	logManager  storage.LogManager
	bufferPool  *storage.BufferPool
	lockManager *LockManager

	nextTxnID atomic.Uint64
	// Pool to recycle transaction contexts
	txnPool sync.Pool
}

// NewTransactionManager initializes the transaction manager.
func NewTransactionManager(logManager storage.LogManager, bufferPool *storage.BufferPool, lockManager *LockManager) *TransactionManager {
	return &TransactionManager{
		logManager: logManager,
		bufferPool: bufferPool,
		lockManager: lockManager,
		activeTxns: xsync.NewMapOf[common.TransactionID, activeTxnEntry](),
	}
}

// Begin starts a new transaction and returns the initialized context.
func (tm *TransactionManager) Begin() (*TransactionContext, error) {
	id := common.TransactionID(tm.nextTxnID.Add(1))
	var txn *TransactionContext
	if pooled := tm.txnPool.Get(); pooled != nil {
		txn = pooled.(*TransactionContext)
		txn.Reset(id)
	} else {
		txn = &TransactionContext{
			id: id,
			lm: tm.lockManager,
			logRecords: newLogRecordBuffer(),
			heldLocks: make(map[DBLockTag]DBLockMode),
		}
	}
	record := txn.NewBeginTransactionRecord()
	lsn, err := tm.logManager.Append(record)
	if err != nil {
		return nil, err
	}
	tm.activeTxns.Store(id, activeTxnEntry{txn: txn, startLsn: lsn})
	return txn, err
}

// Commit completes a transaction and makes its effects durable and visible.
func (tm *TransactionManager) Commit(txn *TransactionContext) error {

	record := txn.NewCommitRecord()
	commitLSN, err := tm.logManager.Append(record)
	if err != nil {
		return err
	}

	// ensure the WAL is on stable storage
	err = tm.logManager.WaitUntilFlushed(commitLSN)
	if err != nil {
		return err
	}

	// Execute In-Memory changes (Indexes) after flushed. Think about how this should interleave with the commit logic.
	for _, task := range txn.commitActions {
		task.Target.Invoke(task.Type, task.Key, task.RID)
	}

	txn.ReleaseAllLocks()
	tm.activeTxns.Delete(txn.id)
	tm.txnPool.Put(txn)

	return nil
}

// Abort stops a transaction and ensures its effects are rolled back
func (tm *TransactionManager) Abort(txn *TransactionContext) error {
	// Rollback In-Memory changes (Indexes)
	// YOU SHOULD NOT NEED TO MODIFY THIS LOGIC
	for i := len(txn.abortActions) - 1; i >= 0; i-- {
		cleanupTask := txn.abortActions[i]
		cleanupTask.Target.Invoke(cleanupTask.Type, cleanupTask.Key, cleanupTask.RID)
	}

	for i := txn.logRecords.len() - 1; i >= 0; i-- {
		record := txn.logRecords.get(i)
		// check record type
		if record.RecordType() == storage.LogInsert {
			clr := txn.NewInsertCLR(record)
			clrLSN, err := tm.logManager.Append(clr)	
			if err != nil {
				return err
			}
			page, err := tm.bufferPool.GetPage(record.RID().PageID)
			if err != nil {
				return err
			}
			page.PageLatch.Lock()
			page.AsHeapPage().MarkDeleted(record.RID(), true)
			page.MonotonicallyUpdateLSN(clrLSN)
			page.PageLatch.Unlock()
			tm.bufferPool.UnpinPage(page, true)
		} else if record.RecordType() == storage.LogDelete {
			clr := txn.NewDeleteCLR(record)
			clrLSN, err := tm.logManager.Append(clr)
			if err != nil {
				return err
			}
			page, err := tm.bufferPool.GetPage(record.RID().PageID)
			if err != nil {
				return err
			}
			page.PageLatch.Lock()
			page.AsHeapPage().MarkDeleted(record.RID(), false)
			page.MonotonicallyUpdateLSN(clrLSN)
			page.PageLatch.Unlock()
			tm.bufferPool.UnpinPage(page, true)
		} else if record.RecordType() == storage.LogUpdate {
			clr := txn.NewUpdateCLR(record)
			clrLSN, err := tm.logManager.Append(clr)
			if err != nil {
				return err
			}
			page, err := tm.bufferPool.GetPage(record.RID().PageID)
			if err != nil {
				return err
			}
			page.PageLatch.Lock()
			copy(page.AsHeapPage().AccessTuple(record.RID()), record.BeforeImage())
			page.MonotonicallyUpdateLSN(clrLSN)
			page.PageLatch.Unlock()
			tm.bufferPool.UnpinPage(page, true)
		}
	}
	abortRecord := txn.NewAbortRecord()
	_, err := tm.logManager.Append(abortRecord)
	if err != nil {
		return err
	}
	txn.ReleaseAllLocks()
	tm.activeTxns.Delete(txn.id)
	tm.txnPool.Put(txn)
	return nil
}

// RestartTransactionForRecovery is used during database recovery (ARIES Analysis phase).
// It reconstructs a TransactionContext for a transaction that was active at the time of the crash.
//
// Hint: You do not need to worry about this function until lab 4
func (tm *TransactionManager) RestartTransactionForRecovery(txnId common.TransactionID) *TransactionContext {
	panic("unimplemented")
}

// ATTEntry represents a snapshot of an active transaction for the Active Transaction Table (ATT).
type ATTEntry struct {
	ID       common.TransactionID
	StartLSN storage.LSN
}

// GetActiveTransactionsSnapshot returns a snapshot of currently active transaction IDs and their start LSNs.
//
// Hint: You do not need to worry about this function until lab 4
func (tm *TransactionManager) GetActiveTransactionsSnapshot() []ATTEntry {
	panic("unimplemented")
}
