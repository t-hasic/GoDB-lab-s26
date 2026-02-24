package execution

import (
	"errors"
	"fmt"

	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// TableHeap represents a physical table stored as a heap file on disk.
// It handles the insertion, update, deletion, and reading of tuples, managing
// interactions with the BufferPool, LockManager, and LogManager.
type TableHeap struct {
	oid         common.ObjectID
	desc        *storage.RawTupleDesc
	bufferPool  *storage.BufferPool
	logManager  storage.LogManager
	lockManager *transaction.LockManager

	numPages int
}

// NewTableHeap creates a TableHeap and performs a metadata scan to initialize stats.
func NewTableHeap(table *catalog.Table, bufferPool *storage.BufferPool, logManager storage.LogManager, lockManager *transaction.LockManager) (*TableHeap, error) {

	dbFile, err := bufferPool.StorageManager().GetDBFile(table.Oid)
	if err != nil {
		return nil, fmt.Errorf("failed to get db file: %w", err)
	}
	numPages, err := dbFile.NumPages()
	if err != nil {
		return nil, fmt.Errorf("failed to get number of pages: %w", err)
	}

	types := make([]common.Type, len(table.Columns))
	for i, column := range table.Columns {
		types[i] = column.Type
	}
	desc := storage.NewRawTupleDesc(types)

	tableHeap := &TableHeap{
		oid: table.Oid,
		desc: desc,
		bufferPool: bufferPool,
		logManager: logManager,
		lockManager: lockManager,
		numPages: numPages,
	}
	return tableHeap, nil
}

// StorageSchema returns the physical byte-layout descriptor of the tuples in this table.
func (tableHeap *TableHeap) StorageSchema() *storage.RawTupleDesc {
	return tableHeap.desc
}

// InsertTuple inserts a tuple into the TableHeap. It should find a free space, allocating if needed, and return the found slot.
func (tableHeap *TableHeap) InsertTuple(txn *transaction.TransactionContext, row storage.RawTuple) (common.RecordID, error) {
	// Get the last page
	if tableHeap.numPages == 0 {
		dbFile, err := tableHeap.bufferPool.StorageManager().GetDBFile(tableHeap.oid)
		if err != nil {
			return common.RecordID{}, fmt.Errorf("failed to get db file: %w", err)
		}
		_, err = dbFile.AllocatePage(1)
		if err != nil {
			return common.RecordID{}, fmt.Errorf("failed to allocate page: %w", err)
		}
		tableHeap.numPages = 1
	}
	pageID := common.PageID{Oid: tableHeap.oid, PageNum: int32(tableHeap.numPages-1)}
	page, err := tableHeap.bufferPool.GetPage(pageID)
	if err != nil {
		return common.RecordID{}, fmt.Errorf("failed to get page: %w", err)
	}
	heapPage := page.AsHeapPage() // page already initialized
	if heapPage.RowSize() == 0 {
		storage.InitializeHeapPage(tableHeap.desc, page)
		heapPage = page.AsHeapPage()
	}
	slotNumber := heapPage.FindFreeSlot()
	if slotNumber == -1 {
		// No free slot, need to allocate a new page
		pageID = common.PageID{Oid: tableHeap.oid, PageNum: int32(tableHeap.numPages)}
		page, err = tableHeap.bufferPool.GetPage(pageID)
		if err != nil {
			return common.RecordID{}, fmt.Errorf("failed to get page: %w", err)
		}
		tableHeap.numPages++

		storage.InitializeHeapPage(tableHeap.desc, page)
		heapPage = page.AsHeapPage()
		slotNumber = heapPage.FindFreeSlot()
	}

	rid := common.RecordID{
		PageID: pageID,
		Slot:   int32(slotNumber),
	}

	dest := heapPage.AccessTuple(rid)
	copy(dest, row)
	heapPage.MarkAllocated(rid, true)

	tableHeap.bufferPool.UnpinPage(page, true)

	return rid, nil
}

var ErrTupleDeleted = errors.New("tuple has been deleted")

func (tableHeap *TableHeap) getRecordPage(rid common.RecordID) (*storage.PageFrame, error) {
	page, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return nil, err
	}
	return page, nil
}

// DeleteTuple marks a tuple as deleted in the TableHeap. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) DeleteTuple(txn *transaction.TransactionContext, rid common.RecordID) error {
	page, err := tableHeap.getRecordPage(rid)
    if err != nil {
        return err
    }
	heapPage := page.AsHeapPage()
	if heapPage.IsDeleted(rid) {
		return ErrTupleDeleted
	}
	heapPage.MarkDeleted(rid, true)
	tableHeap.bufferPool.UnpinPage(page, true)
	return nil
}

// ReadTuple reads the physical bytes of a tuple into the provided buffer. If forUpdate is true, read should acquire
// exclusive lock instead of shared. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) ReadTuple(txn *transaction.TransactionContext, rid common.RecordID, buffer []byte, forUpdate bool) error {
	// TODO: ignored the exclusive locking part
	page, err := tableHeap.getRecordPage(rid)
    if err != nil {
        return err
    }
	heapPage := page.AsHeapPage()
	if heapPage.IsDeleted(rid) {
		return ErrTupleDeleted
	}

	page.PageLatch.Lock()
	dest := heapPage.AccessTuple(rid)
	copy(buffer, dest)
	page.PageLatch.Unlock()

	tableHeap.bufferPool.UnpinPage(page, false)
	return nil
}

// UpdateTuple updates a tuple in-place with new binary data. If the tuple has been deleted, return ErrTupleDeleted.
func (tableHeap *TableHeap) UpdateTuple(txn *transaction.TransactionContext, rid common.RecordID, updatedTuple storage.RawTuple) error {
	page, err := tableHeap.getRecordPage(rid)
    if err != nil {
        return err
    }

	page.PageLatch.Lock()
	heapPage := page.AsHeapPage()
	if heapPage.IsDeleted(rid) {
		return ErrTupleDeleted
	}
	dest := heapPage.AccessTuple(rid)
	copy(dest, updatedTuple)
	page.PageLatch.Unlock()

	tableHeap.bufferPool.UnpinPage(page, true)
	return nil
}

// VacuumPage attempts to clean up deleted slots on a specific page.
// If slots are deleted AND no transaction holds a lock on them, they are marked as free.
// This is used to reclaim space in the background.
func (tableHeap *TableHeap) VacuumPage(pageID common.PageID) error {
	page, err := tableHeap.bufferPool.GetPage(pageID)
	if err != nil {
		return err
	}
	heapPage := page.AsHeapPage()
	for i := 0; i < heapPage.NumSlots(); i++ {
		rid := common.RecordID{ PageID: pageID, Slot: int32(i) }
		if heapPage.IsDeleted(rid) {
			heapPage.MarkAllocated(rid, false)
			// TODO: check if lock is held by any transaction
		}
	}
	tableHeap.bufferPool.UnpinPage(page, true)
	return nil
}

// Iterator creates a new TableHeapIterator to scan the table. It acquires the supplied lock on the table (S, X, or SIX),
// and uses the supplied byte slice to fetch tuples in the returned iterator (for zero-allocation scanning).
func (tableHeap *TableHeap) Iterator(txn *transaction.TransactionContext, mode transaction.DBLockMode, buffer []byte) (TableHeapIterator, error) {
	return TableHeapIterator{
		tableHeap: tableHeap,
		txn: txn,
		mode: mode,
		buffer: buffer,
	}, nil
}

// TableHeapIterator iterates over all valid (allocated and non-deleted) tuples in the heap.
type TableHeapIterator struct {
	// Fill me in!
	tableHeap *TableHeap
	txn *transaction.TransactionContext
	mode transaction.DBLockMode
	buffer []byte
	currentRID common.RecordID
	currentPage *storage.PageFrame
	currentPageNum int
	currentSlot int
	currentError error
}

// IsNil returns true if the TableHeapIterator is the default, uninitialized value
func (it *TableHeapIterator) IsNil() bool {
	return it.tableHeap == nil
}

// Next advances the iterator to the next valid tuple.
// It manages page pins automatically (unpinning the old page when moving to a new one).
func (it *TableHeapIterator) Next() bool {
	for ; it.currentPageNum < it.tableHeap.numPages; {
		// TODO: check if page is allocation map page
		if it.currentPage != nil {
			it.tableHeap.bufferPool.UnpinPage(it.currentPage, false)
		}
		page, err := it.tableHeap.getRecordPage(common.RecordID{ PageID: common.PageID{ Oid: it.tableHeap.oid, PageNum: int32(it.currentPageNum) } })
		it.currentPage = page
		if err != nil {
			it.currentError = err
			return false
		}
		heapPage := page.AsHeapPage()
		for ; it.currentSlot < heapPage.NumSlots(); {
			rid := common.RecordID{ PageID: common.PageID{ Oid: it.tableHeap.oid, PageNum: int32(it.currentPageNum) }, Slot: int32(it.currentSlot) }
			if !heapPage.IsAllocated(rid) {
				it.currentSlot++
				continue
			}
			if heapPage.IsDeleted(rid) {
				it.currentSlot++
				continue
			}
			it.currentRID = rid

			// copy tuple to buffer
			page.PageLatch.Lock()
			dest := heapPage.AccessTuple(rid)
			copy(it.buffer, dest)
			page.PageLatch.Unlock()

			it.currentSlot++
			return true
		}
		it.currentPageNum++
		it.currentSlot = 0
	}
	return false
}

// CurrentTuple returns the raw bytes of the tuple at the current cursor position.
// The bytes are valid only until Next() is called again.
func (it *TableHeapIterator) CurrentTuple() storage.RawTuple {
	return it.buffer
}

// CurrentRID returns the RecordID of the current tuple.
func (it *TableHeapIterator) CurrentRID() common.RecordID {
	return it.currentRID
}

// Error returns the first error encountered during iteration, if any.
func (it *TableHeapIterator) Error() error {
	return it.currentError
}

// Close releases any resources associated with the TableHeapIterator
func (it *TableHeapIterator) Close() error {
	if it.currentPage != nil {
		it.tableHeap.bufferPool.UnpinPage(it.currentPage, false)
	}
	return nil
}
