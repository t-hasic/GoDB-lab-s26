package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SeqScanExecutor implements a sequential scan over a table.
type SeqScanExecutor struct {
	// Fill me in!
	plan *planner.SeqScanNode
	tableHeap *TableHeap
	tableHeapIterator *TableHeapIterator
}

// NewSeqScanExecutor creates a new SeqScanExecutor.
func NewSeqScanExecutor(plan *planner.SeqScanNode, tableHeap *TableHeap) *SeqScanExecutor {
	return &SeqScanExecutor{
		plan: plan,
		tableHeap: tableHeap,
	}
}

func (e *SeqScanExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *SeqScanExecutor) Init(context *ExecutorContext) error {
	// allocate new buffer for one tuple
	bytesPerTuple := e.tableHeap.StorageSchema().BytesPerTuple()
	data := make([]byte, bytesPerTuple)
	iterator, err := e.tableHeap.Iterator(context.GetTransaction(), e.plan.Mode, data)
	if err != nil {
		return err
	}
	e.tableHeapIterator = &iterator
	return err
}

func (e *SeqScanExecutor) Next() bool {
	return e.tableHeapIterator.Next()
}

func (e *SeqScanExecutor) Current() storage.Tuple {
	tuple := storage.FromRawTuple(e.tableHeapIterator.CurrentTuple(), e.tableHeap.StorageSchema(), e.tableHeapIterator.CurrentRID())
	return tuple
}

func (e *SeqScanExecutor) Error() error {
	return e.tableHeapIterator.Error()
}

func (e *SeqScanExecutor) Close() error {
	return e.tableHeapIterator.Close()
}
