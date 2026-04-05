package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// The size of block, in bytes, that the join operator is allowed to buffer
const blockSize = 1 << 15

// BlockNestedLoopJoinExecutor implements the block nested loop join algorithm.
// It loads a block of tuples from the left child into memory and then scans the right child
// to find matches. This reduces the number of times the right child is sequentially scanned.
type BlockNestedLoopJoinExecutor struct {
	// Fill me in!
	plan *planner.NestedLoopJoinNode
	left Executor
	right Executor
	context *ExecutorContext
	// left block state
	leftBlock []storage.Tuple
	leftPos int
	// right scan state
	rightTuple storage.Tuple
	haveRightTuple bool
	// output
	current storage.Tuple
	outBuff []byte
	err error
}

// NewBlockNestedLoopJoinExecutor creates a new BlockNestedLoopJoinExecutor.
func NewBlockNestedLoopJoinExecutor(plan *planner.NestedLoopJoinNode, left Executor, right Executor) *BlockNestedLoopJoinExecutor {
	return &BlockNestedLoopJoinExecutor{
		plan: plan,
		left: left,
		right: right,
	}
}

func (e *BlockNestedLoopJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *BlockNestedLoopJoinExecutor) Init(ctx *ExecutorContext) error {
	e.context = ctx
	// reset state
	e.leftBlock = nil
	e.leftPos = 0
	e.current = storage.Tuple{}
	e.err = nil

	err := e.left.Init(ctx)
	if err != nil {
		return err
	}
	err = e.right.Init(ctx)
	return err
}

func (e *BlockNestedLoopJoinExecutor) Next() bool {
	for {
		if e.err != nil { return false }
		// if current block is empty/exhausted, refill it
		if len(e.leftBlock) == 0 {
			e.leftBlock = e.leftBlock[:0]
			bytesUsed := 0

			leftDesc := storage.NewRawTupleDesc(e.plan.Left.OutputSchema())
			tupleBytes := leftDesc.BytesPerTuple()

			for bytesUsed+tupleBytes <= blockSize && e.left.Next() {
				// child tuple is ephemeral; make a stable copy before storing
				copied := e.left.Current().DeepCopy(leftDesc)
				e.leftBlock = append(e.leftBlock, copied)
				bytesUsed += tupleBytes
			}
			if e.left.Error() != nil { e.err = e.left.Error(); return false }
			// no more left tuples -> join is done
			if len(e.leftBlock) == 0 {
				return false
			}
			e.leftPos = 0
			err := e.right.Close()
			if err != nil {
				e.err = err
				return false
			}
			if err = e.right.Init(e.context); err != nil { e.err = err; return false }
		}
		for {
			if !e.haveRightTuple {
				if !e.right.Next() { break } // right exhausted for this block
				e.haveRightTuple = true
				e.leftPos = 0
			}
			// iterate through the left block
			for e.leftPos < len(e.leftBlock) {
				joinedDesc := storage.NewRawTupleDesc(e.plan.OutputSchema())
				if e.outBuff == nil {
					e.outBuff = make([]byte, joinedDesc.BytesPerTuple())
				}
				
				leftTuple := e.leftBlock[e.leftPos]
				rightTuple := e.right.Current()
				
				joined := storage.MergeTuples(e.outBuff, joinedDesc, leftTuple, rightTuple)

				if planner.ExprIsTrue(e.plan.Predicate.Eval(joined)) {
					e.current = joined.DeepCopy(joinedDesc) // safe for Current()
					e.leftPos++
					return true
				}
				e.leftPos++
			}
			e.leftPos = 0
			e.haveRightTuple = false
		}
		if e.right.Error() != nil {
			e.err = e.right.Error()
			e.leftBlock = nil
			e.leftPos = 0
			return false
		}
		e.leftBlock = nil
		e.leftPos = 0
		e.haveRightTuple = false
	}
}

func (e *BlockNestedLoopJoinExecutor) Current() storage.Tuple {
	return e.current
}

func (e *BlockNestedLoopJoinExecutor) Error() error {
	return e.err
}

func (e *BlockNestedLoopJoinExecutor) Close() error {
	err := e.right.Close()
	if err != nil {
		return err
	}
	err = e.left.Close()
	return err
}
