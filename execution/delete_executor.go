package execution

import (
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/common"
)

// DeleteExecutor executes a DELETE query.
// It iterates over the child (which produces the tuples to be deleted with all rows read),
// removes them from the TableHeap, and cleans up all associated Index entries.
type DeleteExecutor struct {
	// Fill me in!
	plan *planner.DeleteNode
	child Executor
	tableHeap *TableHeap
	indexes []indexing.Index
	context *ExecutorContext
	done bool
	resultTuple storage.Tuple
}

func NewDeleteExecutor(plan *planner.DeleteNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *DeleteExecutor {
	return &DeleteExecutor{
		plan: plan,
		child: child,
		tableHeap: tableHeap,
		indexes: indexes,
		done: false,
	}
}

func (e *DeleteExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *DeleteExecutor) Init(ctx *ExecutorContext) error {
	e.context = ctx
	return e.child.Init(ctx)
}

func (e *DeleteExecutor) Next() bool {
	if e.done {
		return false
	}
	e.done = true

	count := int64(0)
	for e.child.Next() {
		tuple := e.child.Current()
		err := e.tableHeap.DeleteTuple(e.context.txn, tuple.RID())
		if err != nil {
			return false
		}
		for _, idx := range e.indexes {
			md := idx.Metadata()
			keyBuf := make([]byte, md.KeySchema.BytesPerTuple())
			for i, colIdx := range md.ProjectionList {
				val := tuple.GetValue(colIdx)
				md.KeySchema.SetValue(keyBuf, i, val)
			}
			key := md.AsKey(keyBuf)
			err = idx.DeleteEntry(key, tuple.RID(), e.context.txn)
			if err != nil {
				return false
			}
		}
		count++
	}
	e.resultTuple = storage.FromValues(common.NewIntValue(count))
	return true
}

func (e *DeleteExecutor) Current() storage.Tuple {
	return e.resultTuple
}

func (e *DeleteExecutor) Close() error {
	return e.child.Close()
}

func (e *DeleteExecutor) Error() error {
	return e.child.Error()
}
