package execution

import (
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/common"
)

// InsertExecutor executes an INSERT query.
// It consumes tuples from its child (which could be a ValuesExecutor or a SELECT query),
// inserts them into the TableHeap, and updates all associated indexes.
//
// For this course, you may assume that the child does not read from the table you are inserting into
type InsertExecutor struct {
	// Fill me in!
	plan *planner.InsertNode
	child Executor
	tableHeap *TableHeap
	indexes []indexing.Index
	context *ExecutorContext
	done bool
	resultTuple storage.Tuple
}

func NewInsertExecutor(plan *planner.InsertNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *InsertExecutor {
	return &InsertExecutor{
		plan: plan,
		child: child,
		tableHeap: tableHeap,
		indexes: indexes,
		done: false,
	}
}

func (e *InsertExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *InsertExecutor) Init(ctx *ExecutorContext) error {
	e.context = ctx
	return e.child.Init(ctx)
}

func (e *InsertExecutor) Next() bool {
	if e.done {
		return false
	}
	e.done = true

	count := int64(0)
	for e.child.Next() {
		tuple := e.child.Current()
		schema := e.tableHeap.StorageSchema()
		buf := make([]byte, schema.BytesPerTuple())
		tuple.WriteToBuffer(buf, schema)
		rid, err := e.tableHeap.InsertTuple(e.context.txn, buf)
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
			err = idx.InsertEntry(key, rid, e.context.txn)
			if err != nil {
				return false
			}
		}
		count++
	}
	e.resultTuple = storage.FromValues(common.NewIntValue(count))
	return true
}

func (e *InsertExecutor) Current() storage.Tuple {
	return e.resultTuple
}

func (e *InsertExecutor) Close() error {
	return e.child.Close()
}

func (e *InsertExecutor) Error() error {
	return e.child.Error()
}
