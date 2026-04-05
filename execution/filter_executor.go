package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// FilterExecutor filters tuples from its child executor based on a predicate.
type FilterExecutor struct {
	// Fill me in!
	plan *planner.FilterNode
	child Executor
}

// NewFilter creates a new FilterExecutor executor.
func NewFilter(plan *planner.FilterNode, child Executor) *FilterExecutor {
	return &FilterExecutor{
		plan: plan,
		child: child,
	}
}

func (e *FilterExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

// Init initializes the child.
func (e *FilterExecutor) Init(context *ExecutorContext) error {
	return e.child.Init(context)
}

func (e *FilterExecutor) Next() bool {
	for e.child.Next() {
		if planner.ExprIsTrue(e.plan.Predicate.Eval(e.child.Current())) {
			return true
		}
	}
	return false
}

func (e *FilterExecutor) Current() storage.Tuple {
	return e.child.Current()
}

func (e *FilterExecutor) Error() error {
	return e.child.Error()
}

func (e *FilterExecutor) Close() error {
	return e.child.Close()
}
