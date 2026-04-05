package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// HashJoinExecutor implements the hash join algorithm.
// It builds a hash table from the left child and probes it with the right child.
// It only supports Equi-Joins.
type HashJoinExecutor struct {
	// Fill me in!
	plan *planner.HashJoinNode
	left Executor
	right Executor
	hashTable *ExecutionHashTable[[]storage.Tuple]
	err error
	// iterating state
	currentRightTuple storage.Tuple
	currentBucket []storage.Tuple
	bucketPos int
	outBuff []byte
	current storage.Tuple
}

// NewHashJoinExecutor creates a new HashJoinExecutor.
func NewHashJoinExecutor(plan *planner.HashJoinNode, left Executor, right Executor) *HashJoinExecutor {
	return &HashJoinExecutor{
		plan: plan,
		left: left,
		right: right,
	}
}

func (e *HashJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *HashJoinExecutor) Init(ctx *ExecutorContext) error {
	e.err = nil
	e.bucketPos = 0
	e.currentBucket = nil

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
	// init the hash table
	keyTypes := make([]common.Type, len(e.plan.LeftKeys))
	for i, expr := range e.plan.LeftKeys {
		keyTypes[i] = expr.OutputType()
	}
	keySchema := storage.NewRawTupleDesc(keyTypes)
	e.hashTable = NewExecutionHashTable[[]storage.Tuple](keySchema)

	// build hash table from the left child
	leftDesc := storage.NewRawTupleDesc(e.plan.Left.OutputSchema())
	for e.left.Next() {
		leftTuple := e.left.Current()
		leftTuple = leftTuple.DeepCopy(leftDesc)
		// construct key
		keyValues := make([]common.Value, len(e.plan.LeftKeys))
		for i, expr := range e.plan.LeftKeys {
			keyValues[i] = expr.Eval(leftTuple)
		}
		hasNull := false
		for _, v := range keyValues {
			if v.IsNull() {
				hasNull = true
				break
			}
		}
		if hasNull {
			continue
		}
		keyTuple := storage.FromValues(keyValues...)

		// insert key,tuple into hash_map
		bucket, _ := e.hashTable.Get(keyTuple)
		bucket = append(bucket, leftTuple)
		e.hashTable.Insert(keyTuple, bucket)
	}
	if e.left.Error() != nil {
		e.err = e.left.Error()
		return e.err
	}
	return nil
}

func (e *HashJoinExecutor) Next() bool {
	for {
        // If we still have unemitted matches in the current bucket, emit the next one
        for e.bucketPos < len(e.currentBucket) {
            leftTuple := e.currentBucket[e.bucketPos]
            e.bucketPos++

			// join tuples
			joinedDesc := storage.NewRawTupleDesc(e.plan.OutputSchema())
			if e.outBuff == nil {
				e.outBuff = make([]byte, joinedDesc.BytesPerTuple())
			}
			e.current = storage.MergeTuples(e.outBuff, joinedDesc, leftTuple, e.currentRightTuple)
            return true
        }

        // Current bucket exhausted — advance to next right tuple
        if !e.right.Next() {
            return false
        }
        e.currentRightTuple = e.right.Current()

        // Probe the hash table
        keyValues := make([]common.Value, len(e.plan.RightKeys))
        for i, expr := range e.plan.RightKeys {
			keyValues[i] = expr.Eval(e.currentRightTuple)
		}
		hasNull := false
		for _, v := range keyValues {
			if v.IsNull() {
				hasNull = true
				break
			}
		}
		if hasNull {
			e.currentBucket = nil
			e.bucketPos = 0
			continue
		}
		keyTuple := storage.FromValues(keyValues...)
        e.currentBucket, _ = e.hashTable.Get(keyTuple)
        e.bucketPos = 0
    }
}

func (e *HashJoinExecutor) Current() storage.Tuple {
	return e.current
}

func (e *HashJoinExecutor) Error() error {
	return e.err
}

func (e *HashJoinExecutor) Close() error {
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
