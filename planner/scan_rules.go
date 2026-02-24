package planner

import (
	"fmt"

	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

type SeqScanRule struct{}

func (r *SeqScanRule) Name() string {
	return "SeqScanRule"
}

func (r *SeqScanRule) Match(node LogicalPlanNode, children []PlanNode, catalog *catalog.Catalog) bool {
	_, ok := node.(*LogicalScanNode)
	return ok
}

func (r *SeqScanRule) Priority() int {
	return 0
}

func (r *SeqScanRule) Apply(node LogicalPlanNode, children []PlanNode, catalog *catalog.Catalog, exprBinder *ExpressionBinder) (PlanNode, error) {
	scanNode := node.(*LogicalScanNode)
	table, err := catalog.GetTableByOid(scanNode.GetTableOid())
	if err != nil {
		return nil, err
	}
	lockMode := transaction.LockModeS
	if scanNode.ForUpdate {
		lockMode = transaction.LockModeX
	}
	physicalNode := NewSeqScanNode(scanNode.GetTableOid(), tableSchemaToTypes(table), lockMode)
	if len(scanNode.Predicates) > 0 {
		return wrapInFilter(physicalNode, scanNode.Predicates, scanNode.OutputSchema(), exprBinder), nil
	}
	return physicalNode, nil
}

func isEqualityScan(expr Expr, tableAlias string) (*LogicalColumn, common.Value, bool) {
	if cmp, ok := expr.(*ComparisonExpression); ok && cmp.compType == Equal {
		if col, ok := cmp.left.(*LogicalColumn); ok {
			if val, ok := cmp.right.(*ConstantValueExpr); ok {
				if col.origin.alias == tableAlias {
					return col, val.val, true
				}
			}
		}
		if col, ok := cmp.right.(*LogicalColumn); ok {
			if val, ok := cmp.left.(*ConstantValueExpr); ok {
				if col.origin.alias == tableAlias {
					return col, val.val, true
				}
			}
		}
	}
	return nil, common.NewNullInt(), false
}

func isRangeScan(expr Expr, tableAlias string) (*LogicalColumn, common.Value, ComparisonType, bool) {
	cmp, ok := expr.(*ComparisonExpression)
	if !ok {
		return nil, common.NewNullInt(), 0, false
	}

	if col, ok := cmp.left.(*LogicalColumn); ok {
		if val, ok := cmp.right.(*ConstantValueExpr); ok {
			if col.origin.alias == tableAlias {
				return col, val.val, cmp.compType, true
			}
		}
	}

	if col, ok := cmp.right.(*LogicalColumn); ok {
		if val, ok := cmp.left.(*ConstantValueExpr); ok {
			if col.origin.alias == tableAlias {
				flippedOp := flipComparison(cmp.compType)
				return col, val.val, flippedOp, true
			}
		}
	}

	return nil, common.NewNullInt(), 0, false
}

func flipComparison(op ComparisonType) ComparisonType {
	switch op {
	case GreaterThan:
		return LessThan
	case GreaterThanOrEqual:
		return LessThanOrEqual
	case LessThan:
		return GreaterThan
	case LessThanOrEqual:
		return GreaterThanOrEqual
	}
	return op // Equal / NotEqual stay the same
}

// IndexMatchResult describes how well an index matches the scan predicates
type IndexMatchResult struct {
	NumMatchedColumns int
	Values            []common.Value
	IsRange           bool
	RangeOp           ComparisonType
	MatchedPreds      []Expr
}

// analyzeIndex checks if the given index can be used to satisfy the scan predicates.
// Matches columns from left to right without gaps.
// TODO: Normalize predicates. E.g. We don't want predicates like col = 5 AND col > 3 to prevent
// confusion in index matching.
func analyzeIndex(idx *catalog.Index, predicates []Expr, tableAlias string) *IndexMatchResult {
	result := &IndexMatchResult{
		Values:       make([]common.Value, 0),
		MatchedPreds: make([]Expr, 0),
	}

	for _, colName := range idx.KeySchema {
		foundMatch := false

		// Check for equality scans first (e.g., col = 5).
		for _, pred := range predicates {
			if col, val, ok := isEqualityScan(pred, tableAlias); ok {
				if col.cname == colName {
					result.NumMatchedColumns++
					result.Values = append(result.Values, val)
					result.MatchedPreds = append(result.MatchedPreds, pred)
					foundMatch = true
					break
				}
			}
		}

		if foundMatch {
			continue
		}

		// If no equality, check for range scan (for the last matched column)
		for _, pred := range predicates {
			if col, val, op, ok := isRangeScan(pred, tableAlias); ok {
				if col.cname == colName {
					result.NumMatchedColumns++
					result.Values = append(result.Values, val)
					result.MatchedPreds = append(result.MatchedPreds, pred)
					result.IsRange = true
					result.RangeOp = op
					return result
				}
			}
		}

		// Assume we cannot skip columns in the index.
		break
	}

	return result
}

// makeMultiColumnKey constructs a composite key from a list of values
func makeMultiColumnKey(values []common.Value, types []common.Type) (indexing.Key, error) {
	if len(values) != len(types) {
		return indexing.Key{}, fmt.Errorf("key construction mismatch: got %d values for %d types", len(values), len(types))
	}

	// Create a tuple desc for the key
	desc := storage.NewRawTupleDesc(types)
	buf := make([]byte, desc.BytesPerTuple())

	for i, val := range values {
		desc.SetValue(buf, i, val)
	}
	return indexing.NewKey(buf, desc), nil
}

type IndexScanRule struct{}

func (r *IndexScanRule) Name() string { return "IndexScanRule" }

func (r *IndexScanRule) Priority() int { return 1 }

func (r *IndexScanRule) Match(node LogicalPlanNode, children []PlanNode, c *catalog.Catalog) bool {
	scan, ok := node.(*LogicalScanNode)
	if !ok || len(scan.Predicates) == 0 {
		return false
	}

	table, err := c.GetTableByOid(scan.GetTableOid())
	if err != nil {
		return false
	}

	for _, idx := range table.Indexes {
		match := analyzeIndex(&idx, scan.Predicates, scan.GetTableAlias())
		if match.NumMatchedColumns > 0 && match.IsRange {
			return true
		}
	}
	return false
}

func (r *IndexScanRule) Apply(node LogicalPlanNode, children []PlanNode, c *catalog.Catalog, exprBinder *ExpressionBinder) (PlanNode, error) {
	scan := node.(*LogicalScanNode)
	table, _ := c.GetTableByOid(scan.GetTableOid())

	var bestIdx *catalog.Index
	var bestMatch *IndexMatchResult

	for _, idx := range table.Indexes {
		currentIdx := idx
		match := analyzeIndex(&currentIdx, scan.Predicates, scan.GetTableAlias())

		if match.NumMatchedColumns > 0 && match.IsRange {
			if bestMatch == nil || match.NumMatchedColumns > bestMatch.NumMatchedColumns {
				bestMatch = match
				bestIdx = &currentIdx
			}
		}
	}

	if bestIdx == nil {
		return nil, fmt.Errorf("IndexScanRule matched but failed to find index during Apply")
	}

	direction := indexing.ScanDirectionForward
	op := bestMatch.RangeOp
	if op == LessThan || op == LessThanOrEqual {
		direction = indexing.ScanDirectionBackward
	}

	keyTypes := make([]common.Type, bestMatch.NumMatchedColumns)
	for i := 0; i < bestMatch.NumMatchedColumns; i++ {
		colName := bestIdx.KeySchema[i]
		for _, colDef := range table.Columns {
			if colDef.Name == colName {
				keyTypes[i] = colDef.Type
				break
			}
		}
	}
	key, err := makeMultiColumnKey(bestMatch.Values, keyTypes)
	if err != nil {
		return nil, err
	}

	var plan PlanNode = NewIndexScanNode(
		bestIdx.Oid,
		scan.GetTableOid(),
		tableSchemaToTypes(table),
		direction,
		key,
		scan.ForUpdate,
	)

	// Wrap in filter for any residual predicates.
	// TODO: exclude bestMatch.MatchedPreds from the filter
	if len(scan.Predicates) > 0 {
		plan = wrapInFilter(plan, scan.Predicates, scan.OutputSchema(), exprBinder)
	}

	return plan, nil
}

type IndexLookupRule struct{}

func (r *IndexLookupRule) Name() string {
	return "IndexLookupRule"
}

func (r *IndexLookupRule) Match(node LogicalPlanNode, children []PlanNode, c *catalog.Catalog) bool {
	scan, ok := node.(*LogicalScanNode)
	if !ok || len(scan.Predicates) == 0 {
		return false
	}

	table, err := c.GetTableByOid(scan.GetTableOid())
	if err != nil {
		return false
	}

	// Look for any index that supports a pure equality lookup
	for _, idx := range table.Indexes {
		match := analyzeIndex(&idx, scan.Predicates, scan.GetTableAlias())
		if match.NumMatchedColumns > 0 && !match.IsRange {
			return true
		}
	}
	return false
}

func (r *IndexLookupRule) Priority() int {
	return 2
}

func (r *IndexLookupRule) Apply(node LogicalPlanNode, children []PlanNode, c *catalog.Catalog, exprBinder *ExpressionBinder) (PlanNode, error) {
	scan := node.(*LogicalScanNode)
	table, _ := c.GetTableByOid(scan.GetTableOid())

	var bestIdx *catalog.Index
	var bestMatch *IndexMatchResult

	for _, idx := range table.Indexes {
		currentIdx := idx
		match := analyzeIndex(&currentIdx, scan.Predicates, scan.GetTableAlias())

		if match.NumMatchedColumns > 0 && !match.IsRange {
			if bestMatch == nil || match.NumMatchedColumns > bestMatch.NumMatchedColumns {
				bestMatch = match
				bestIdx = &currentIdx
			}
		}
	}

	if bestIdx == nil {
		return nil, fmt.Errorf("IndexLookupRule matched but failed to find index during Apply")
	}

	keyTypes := make([]common.Type, bestMatch.NumMatchedColumns)
	for i := 0; i < bestMatch.NumMatchedColumns; i++ {
		// Find the column definition in the table to get its Type
		colName := bestIdx.KeySchema[i]
		for _, colDef := range table.Columns {
			if colDef.Name == colName {
				keyTypes[i] = colDef.Type
				break
			}
		}
	}
	key, err := makeMultiColumnKey(bestMatch.Values, keyTypes)
	if err != nil {
		return nil, err
	}

	var plan PlanNode = NewIndexLookupNode(
		bestIdx.Oid,
		scan.GetTableOid(),
		tableSchemaToTypes(table),
		key,
		scan.ForUpdate,
	)

	// Wrap in filter for any residual predicates.
	// TODO: exclude bestMatch.MatchedPreds from the filter
	if len(scan.Predicates) > 0 {
		plan = wrapInFilter(plan, scan.Predicates, scan.OutputSchema(), exprBinder)
	}

	return plan, nil
}
