package execution

import (
	"sort"

	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SortExecutor sorts the input tuples based on the provided ordering expressions.
// It is a blocking operator but uses lazy evaluation (sorts on first Next).
type SortExecutor struct {
	// Fill me in!
	plan *planner.SortNode
	child Executor
	tuples []storage.Tuple
	cursor int
	err error
}

func NewSortExecutor(plan *planner.SortNode, child Executor) *SortExecutor {
	return &SortExecutor{
		plan: plan,
		child: child,
	}
}

func (e *SortExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *SortExecutor) compareTuples(a, b storage.Tuple) int {
	// return -1 if a < b, 0 if equal, +1 if a > b
	for _, ob := range e.plan.OrderBy {
		cmp := ob.Expr.Eval(a).Compare(ob.Expr.Eval(b))
		if cmp == 0 {
			continue
		}
		if ob.Direction == planner.SortOrderAscending {
			return cmp
		}
		return -cmp // reverse for DESC
	}
	return 0
}

func (e *SortExecutor) Init(ctx *ExecutorContext) error {
	e.tuples = nil
	e.cursor = 0
	err := e.child.Init(ctx)
	e.err = err
	if e.err != nil {
		return e.err
	}

	// blocking phase: consume entire child
	desc := storage.NewRawTupleDesc(e.plan.OutputSchema())
	for e.child.Next() {
		// DeepCopy is important because child tuples may be backed by reused buffers
		t := e.child.Current().DeepCopy(desc)
		e.tuples = append(e.tuples, t)
	}

	if err := e.child.Error(); err != nil {
		e.err = err
		return err
	}

	sort.SliceStable(e.tuples, func(i, j int) bool {
		return e.compareTuples(e.tuples[i], e.tuples[j]) < 0
	})
	return nil
}

func (e *SortExecutor) Next() bool {
	if e.cursor < len(e.tuples) {
		e.cursor++
		return true
	}
	return false
}

func (e *SortExecutor) Current() storage.Tuple {
	return e.tuples[e.cursor-1]
}

func (e *SortExecutor) Error() error {
	return e.err
}

func (e *SortExecutor) Close() error {
	return e.child.Close()
}
