package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// LimitExecutor limits the number of tuples returned by the child executor.
type LimitExecutor struct {
	// Fill me in!
	plan *planner.LimitNode
	child Executor
	totalEmitted int
}

func NewLimitExecutor(plan *planner.LimitNode, child Executor) *LimitExecutor {
	return &LimitExecutor{
		plan: plan,
		child: child,
		totalEmitted: 0,
	}
}

func (e *LimitExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *LimitExecutor) Init(ctx *ExecutorContext) error {
	e.totalEmitted = 0
	return e.child.Init(ctx)
}

func (e *LimitExecutor) Next() bool {
	if e.totalEmitted < e.plan.Limit {
		if e.child.Next() {
			e.totalEmitted++
			return true
		}
		return false
	}
	return false
}

func (e *LimitExecutor) Current() storage.Tuple {
	return e.child.Current()
}

func (e *LimitExecutor) Error() error {
	return e.child.Error()
}

func (e *LimitExecutor) Close() error {
	return e.child.Close()
}
