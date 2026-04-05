package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SortMergeJoinExecutor implements the sort-merge join algorithm.
// The planner guarantees that both children are already sorted on their join keys.
// You only need to support Equi-Joins
type SortMergeJoinExecutor struct {
	// Fill me in!
	plan *planner.SortMergeJoinNode
	left Executor
	right Executor
	err error
	leftValid  bool              
	rightValid bool              
	rightGroup []storage.Tuple
	rightGroupIdx int            
	current storage.Tuple
	outBuff []byte
}

func NewSortMergeJoinExecutor(plan *planner.SortMergeJoinNode, left, right Executor) *SortMergeJoinExecutor {
	return &SortMergeJoinExecutor{
		plan: plan,
		left: left,
		right: right,
	}
}

func (e *SortMergeJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *SortMergeJoinExecutor) Init(ctx *ExecutorContext) error {
	e.err = nil

	err := e.left.Init(ctx)
	if err != nil {
		e.err = err
		return err
	}
	err = e.right.Init(ctx)
	if err != nil {
		e.err = err
		return err
	}
	e.leftValid = e.left.Next()
	if e.left.Error() != nil {
		e.err = e.left.Error()
		return err
	}
	e.rightValid = e.right.Next()
	if e.right.Error() != nil {
		e.err = e.right.Error()
		return err
	}
	e.rightGroup = nil
	e.rightGroupIdx = 0

	return nil
}

func (e *SortMergeJoinExecutor) compareKeys(leftTuple, rightTuple storage.Tuple) int {
    for i := range e.plan.LeftKeys {
        leftVal := e.plan.LeftKeys[i].Eval(leftTuple)
        rightVal := e.plan.RightKeys[i].Eval(rightTuple)
        cmp := leftVal.Compare(rightVal)
        if cmp != 0 {
            return cmp
        }
    }
    return 0
}

func (e *SortMergeJoinExecutor) hasNullKeys(tuple storage.Tuple, keys []planner.Expr) bool {
    for _, key := range keys {
        if key.Eval(tuple).IsNull() {
            return true
        }
    }
    return false
}

func (e *SortMergeJoinExecutor) Next() bool {
	for {
		if e.rightGroupIdx < len(e.rightGroup) {
			// merge left.Current() with rightGroup[rightGroupIdx] → store in e.current
			joinedDesc := storage.NewRawTupleDesc(e.plan.OutputSchema())
			if e.outBuff == nil {
				e.outBuff = make([]byte, joinedDesc.BytesPerTuple())
			}
			e.current = storage.MergeTuples(e.outBuff, joinedDesc, e.left.Current(), e.rightGroup[e.rightGroupIdx])
			e.rightGroupIdx++
			return true
		}
		// check if left still will match with our right gruop
		if len(e.rightGroup) > 0 {
			e.leftValid = e.left.Next()
			if e.left.Error() != nil {
				e.err = e.left.Error()
				return false
			}
			if !e.leftValid {
				return false
			}
			if e.compareKeys(e.left.Current(), e.rightGroup[0]) == 0 {
				e.rightGroupIdx = 0
				continue // emit from right gruop again (left still matches)
			}

			e.rightGroup = nil
		}

		for e.leftValid && e.rightValid {
			cmp := e.compareKeys(e.left.Current(), e.right.Current())
	
			if cmp < 0 {
				e.leftValid = e.left.Next()
				if e.left.Error() != nil {
					e.err = e.left.Error()
					return false
				}
			} else if cmp > 0 {
				e.rightValid = e.right.Next() 
				if e.right.Error() != nil {
					e.err = e.right.Error()
					return false
				}
			} else {
				if e.hasNullKeys(e.left.Current(), e.plan.LeftKeys) ||
				e.hasNullKeys(e.right.Current(), e.plan.RightKeys) {
					// Advance both past the NULLs (they sort to the front)
					e.leftValid = e.left.Next()
					e.rightValid = e.right.Next()
					if e.left.Error() != nil {
						e.err = e.left.Error()
						return false
					}
					if e.right.Error() != nil {
						e.err = e.right.Error()
						return false
					}
					continue
				}
				e.rightGroup = make([]storage.Tuple, 0)
				rightDesc := storage.NewRawTupleDesc(e.plan.Right.OutputSchema())
				for e.rightValid && e.compareKeys(e.left.Current(), e.right.Current()) == 0 {
					e.rightGroup = append(e.rightGroup, e.right.Current().DeepCopy(rightDesc))
					e.rightValid = e.right.Next()
					if e.right.Error() != nil {
						e.err = e.right.Error()
						return false
					}
				}
				e.rightGroupIdx = 0
				break
			}
		}
		if len(e.rightGroup) == 0 {
			return false
		}
	}
}

func (e *SortMergeJoinExecutor) Current() storage.Tuple {
	return e.current
}

func (e *SortMergeJoinExecutor) Error() error {
	return e.err
}

func (e *SortMergeJoinExecutor) Close() error {
	err := e.left.Close()
	if err != nil {
		e.err = err
		return err
	}
	err = e.right.Close()
	if err != nil {
		e.err = err
		return err
	}	
	return nil
}
