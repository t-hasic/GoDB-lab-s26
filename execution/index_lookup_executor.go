package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// IndexLookupExecutor implements a Point Lookup using an index. Unlike a full Index Scan, which iterates over a
// range of keys, this executor efficiently retrieves only the tuples that match a specific equality key
// (e.g., "SELECT * FROM users WHERE id = 5").
type IndexLookupExecutor struct {
	// Fill me in!
	plan *planner.IndexLookupNode
	index indexing.Index
	tableHeap *TableHeap
	context *ExecutorContext
	matchingRIDs []common.RecordID
	currentRID common.RecordID
	currentTuple storage.Tuple
	pos int
	err error
}

func NewIndexLookupExecutor(plan *planner.IndexLookupNode, index indexing.Index, tableHeap *TableHeap) *IndexLookupExecutor {
	return &IndexLookupExecutor{
		plan: plan,
		index: index,
		tableHeap: tableHeap,
	}
}

func (e *IndexLookupExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *IndexLookupExecutor) Init(ctx *ExecutorContext) error {
	e.context = ctx
	e.matchingRIDs, e.err = e.index.ScanKey(e.plan.EqualityKey, e.matchingRIDs[:0], ctx.GetTransaction())
	if e.err != nil {
		return e.err
	}
	return e.err
}

func (e *IndexLookupExecutor) Next() bool {
	if e.err != nil {
		return false
	}

	for e.pos < len(e.matchingRIDs) {
		e.currentRID = e.matchingRIDs[e.pos]

		bytesPerTuple := e.tableHeap.StorageSchema().BytesPerTuple()
		buffer := make([]byte, bytesPerTuple)
		err := e.tableHeap.ReadTuple(e.context.txn, e.currentRID, buffer, e.plan.ForUpdate)
		if err == ErrTupleDeleted {
			e.pos++
			continue
		}
		if err != nil {
			e.err = err
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
		if !fetchedKey.Equals(e.plan.EqualityKey) {
			e.pos++
			continue
		}
		e.currentTuple = storage.FromRawTuple(buffer, e.tableHeap.StorageSchema(), e.currentRID)
		e.pos++
		return true
	}
	return false
}

func (e *IndexLookupExecutor) Current() storage.Tuple {
	return e.currentTuple
}

func (e *IndexLookupExecutor) Close() error {
	return nil
}

func (e *IndexLookupExecutor) Error() error {
	return e.err
}
