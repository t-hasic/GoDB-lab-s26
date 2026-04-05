package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// AggregateExecutor implements hash-based aggregation.
type AggregateExecutor struct {
	// Fill me in!
	plan *planner.AggregateNode
	child Executor
	hashTable *ExecutionHashTable[[]common.Value]
	err error
	cursor int
	results []storage.Tuple
}

func NewAggregateExecutor(plan *planner.AggregateNode, child Executor) *AggregateExecutor {
	return &AggregateExecutor{
		plan: plan,
		child: child,
	}
}

func (e *AggregateExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *AggregateExecutor) Init(ctx *ExecutorContext) error {
	e.err = nil
	e.cursor = 0
	e.results = nil
	err := e.child.Init(ctx)
	if err != nil {
		e.err = err
		return err
	}
	groupByTypes := make([]common.Type, len(e.plan.GroupByClause))
	for i, expr := range e.plan.GroupByClause {
		groupByTypes[i] = expr.OutputType()
	}
	keySchema := storage.NewRawTupleDesc(groupByTypes)
	e.hashTable = NewExecutionHashTable[[]common.Value](keySchema)

	for e.child.Next() {
		t := e.child.Current()
	
		// Build the group-by key
		keyValues := make([]common.Value, len(e.plan.GroupByClause))
		for i, expr := range e.plan.GroupByClause {
			keyValues[i] = expr.Eval(t)
		}
		keyTuple := storage.FromValues(keyValues...)
	
		// Lookup or create the aggregate accumulators
		aggs, exists := e.hashTable.Get(keyTuple)
		if !exists {
			aggs = make([]common.Value, len(e.plan.AggClauses))
		}

		for i, agg := range e.plan.AggClauses {
			aggVal := agg.Expr.Eval(t)
		
			if !exists {
				// First tuple for this group — initialize
				switch agg.Type {
				case planner.AggCount:
					if aggVal.IsNull() {
						aggs[i] = common.NewIntValue(0)
					} else {
						aggs[i] = common.NewIntValue(1)
					}
				default:
					// For SUM/MIN/MAX: store the value as-is (could be NULL)
					// If it's NULL, it acts as "no value seen yet"
					aggs[i] = aggVal.Copy()
				}
			} else {
				// Group already exists — update
				if aggVal.IsNull() {
					// NULL values are ignored in aggregation; skip
					continue
				}
		
				switch agg.Type {
				case planner.AggCount:
					aggs[i] = common.NewIntValue(aggs[i].IntValue() + 1)
				case planner.AggSum:
					if aggs[i].IsNull() {
						// First non-NULL value for this group's SUM
						aggs[i] = aggVal
					} else {
						aggs[i] = common.NewIntValue(aggs[i].IntValue() + aggVal.IntValue())
					}
				case planner.AggMin:
					if aggs[i].IsNull() || aggVal.Compare(aggs[i]) < 0 {
						aggs[i] = aggVal.Copy()
					}
				case planner.AggMax:
					if aggs[i].IsNull() || aggVal.Compare(aggs[i]) > 0 {
						aggs[i] = aggVal.Copy()
					}
				}
			}
		}

		e.hashTable.Insert(keyTuple, aggs)
	}
	if e.child.Error() != nil {
		e.err = e.child.Error()
		return e.err
	}
	e.hashTable.Iterate(func(key storage.Tuple, aggs []common.Value) {
		// Extract the group-by values from the key tuple
		groupByValues := make([]common.Value, len(e.plan.GroupByClause))
		for i := range groupByValues {
			groupByValues[i] = key.GetValue(i)
		}
	
		// Build the output tuple: [groupBy1, groupBy2, ..., agg1, agg2, ...]
		allValues := append(groupByValues, aggs...)
		e.results = append(e.results, storage.FromValues(allValues...))
	})
	return nil
}

func (e *AggregateExecutor) Next() bool {
    if e.cursor < len(e.results) {
        e.cursor++
        return true
    }
    return false
}

func (e *AggregateExecutor) Current() storage.Tuple {
    return e.results[e.cursor-1]
}

func (e *AggregateExecutor) Error() error {
    return e.err
}

func (e *AggregateExecutor) Close() error {
    return e.child.Close()
}