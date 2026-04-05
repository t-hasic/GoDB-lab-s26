package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// ProjectionExecutor evaluates a list of expressions on the input tuples
// and produces a new tuple containing the results of those expressions.
type ProjectionExecutor struct {
	// Fill me in!
	plan *planner.ProjectionNode
	child Executor
}

// NewProjectionExecutor creates a new ProjectionExecutor.
func NewProjectionExecutor(plan *planner.ProjectionNode, child Executor) *ProjectionExecutor {
	return &ProjectionExecutor{
		plan: plan,
		child: child,
	}
}

func (e *ProjectionExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *ProjectionExecutor) Init(ctx *ExecutorContext) error {
	return e.child.Init(ctx)
}

func (e *ProjectionExecutor) Next() bool {
	return e.child.Next()
}

func (e *ProjectionExecutor) Current() storage.Tuple {
	currentTuple := e.child.Current()
	values := make([]common.Value, len(e.plan.Expressions))
	for i, expr := range e.plan.Expressions {
		values[i] = expr.Eval(currentTuple)
	}
	return storage.FromValues(values...)
}

func (e *ProjectionExecutor) Error() error {
	return e.child.Error()
}

func (e *ProjectionExecutor) Close() error {
	return e.child.Close()
}
