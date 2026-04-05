package execution

import (
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// IndexScanExecutor executes a range scan over an index.
// It iterates through the B+Tree (or other index type) starting from a specific key
// and traversing in a specific direction (Forward or Backward).
type IndexScanExecutor struct {
	// Fill me in!
	plan *planner.IndexScanNode
	index indexing.Index
	tableHeap *TableHeap
	iterator indexing.ScanIterator
	context *ExecutorContext
	currentTuple storage.Tuple
}

func NewIndexScanExecutor(plan *planner.IndexScanNode, index indexing.Index, tableHeap *TableHeap) *IndexScanExecutor {
	return &IndexScanExecutor{
		plan: plan,
		index: index,
		tableHeap: tableHeap,
	}
}

func (e *IndexScanExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *IndexScanExecutor) Init(ctx *ExecutorContext) error {
	e.context = ctx
	iterator, err := e.index.Scan(
		e.plan.StartKey,
		e.plan.Direction,
		ctx.txn,
	)
	if err != nil {
		return err
	}
	e.iterator = iterator
	return err
}

func (e *IndexScanExecutor) Next() bool {
	for {
		if !e.iterator.Next() {
			return false
		}
		rid := e.iterator.Value()
		bytesPerTuple := e.tableHeap.StorageSchema().BytesPerTuple()
		buffer := make([]byte, bytesPerTuple)
		err := e.tableHeap.ReadTuple(e.context.txn, rid, buffer, e.plan.ForUpdate)
		if err == ErrTupleDeleted {
			continue
		}
		if err != nil {
			return false
		}

		// recheck the key
		md := e.index.Metadata()
		keyBuf := make([]byte, md.KeySchema.BytesPerTuple())
		tableDesc := e.tableHeap.StorageSchema()
		for i, col := range md.ProjectionList {
			srcOff := tableDesc.GetFieldOffset(col)
			srcSize := tableDesc.GetFieldType(col).Size()
			dstOff := md.KeySchema.GetFieldOffset(i)
			copy(keyBuf[dstOff:dstOff+srcSize], buffer[srcOff:srcOff+srcSize])
		}
		fetchedKey := md.AsKey(keyBuf)
		if !fetchedKey.Equals(e.iterator.Key()) {
			continue
		}
		e.currentTuple = storage.FromRawTuple(buffer, e.tableHeap.StorageSchema(), rid)
		return true
	}
	return false
}

func (e *IndexScanExecutor) Current() storage.Tuple {
	return e.currentTuple
}

func (e *IndexScanExecutor) Close() error {
	return e.iterator.Close()
}

func (e *IndexScanExecutor) Error() error {
	return e.iterator.Error()
}
