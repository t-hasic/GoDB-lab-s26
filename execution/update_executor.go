package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// UpdateExecutor implements the execution logic for updating tuples in a table.
// It iterates over the tuples provided by its child executor, which represent the full value of the current row
// and its RID. It uses the expressions defined in the plan to calculate the new values for every column in the new row.
// The executor updates the table heap in-place and ensures that all relevant indexes are updated
// if the key columns have changed. It produces a single tuple containing the count of updated rows.
type UpdateExecutor struct {
	// Fill me in!
	plan *planner.UpdateNode
	child Executor
	tableHeap *TableHeap
	indexes []indexing.Index
	context *ExecutorContext
	done bool
	resultTuple storage.Tuple
}

func NewUpdateExecutor(plan *planner.UpdateNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *UpdateExecutor {
	return &UpdateExecutor{
		plan: plan,
		child: child,
		tableHeap: tableHeap,
		indexes: indexes,
		done: false,
	}
}

func (e *UpdateExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *UpdateExecutor) Init(ctx *ExecutorContext) error {
	e.context = ctx
	return e.child.Init(ctx)
}

func (e *UpdateExecutor) Next() bool {
	if e.done {
		return false
	}
	e.done = true

	count := int64(0)
	for e.child.Next() {
		tuple := e.child.Current()
		rid := tuple.RID()

		schema := e.tableHeap.StorageSchema()
		newBuf := make([]byte, schema.BytesPerTuple())
		for i, expr := range e.plan.Expressions {
			val := expr.Eval(tuple)
			schema.SetValue(newBuf, i, val)
		}
		err := e.tableHeap.UpdateTuple(e.context.txn, rid, newBuf)

		if err != nil {
			return false
		}

		for _, idx := range e.indexes {
			md := idx.Metadata()
		
			// Build old key from old tuple
			oldKeyBuf := make([]byte, md.KeySchema.BytesPerTuple())
			for i, colIdx := range md.ProjectionList {
				val := tuple.GetValue(colIdx)
				md.KeySchema.SetValue(oldKeyBuf, i, val)
			}
			oldKey := md.AsKey(oldKeyBuf)
		
			// Build new key from evaluated expressions
			newKeyBuf := make([]byte, md.KeySchema.BytesPerTuple())
			for i, colIdx := range md.ProjectionList {
				val := e.plan.Expressions[colIdx].Eval(tuple)
				md.KeySchema.SetValue(newKeyBuf, i, val)
			}
			newKey := md.AsKey(newKeyBuf)
			if oldKey.Equals(newKey) {
				continue
			}
		
			// Delete old index entry, insert new one
			err = idx.DeleteEntry(oldKey, rid, e.context.txn)
			if err != nil {
				return false
			}
			err = idx.InsertEntry(newKey, rid, e.context.txn)
			if err != nil {
				return false
			}
		}
		count++
	}
	e.resultTuple = storage.FromValues(common.NewIntValue(count))
	return true
}

func (e *UpdateExecutor) OutputSchema() []common.Type {
	return e.plan.OutputSchema()
}

func (e *UpdateExecutor) Current() storage.Tuple {
	return e.resultTuple
}

func (e *UpdateExecutor) Close() error {
	return e.child.Close()
}

func (e *UpdateExecutor) Error() error {
	return e.child.Error()
}
