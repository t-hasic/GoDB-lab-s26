package planner

import (
	"fmt"
	"strings"

	"mit.edu/dsg/godb/common"
)

type LogicalScanNode struct {
	BaseLogicalPlanNode

	TableRef *TableRef

	// The optimizer may mutate outputSchema.
	outputSchema LogicalSchema

	Predicates []Expr
	ForUpdate  bool // Indicates if this scan is for an UPDATE statement (used for locking)
}

func NewLogicalScanNode(tableRef *TableRef, forUpdate bool) *LogicalScanNode {
	if tableRef == nil {
		panic("NewLogicalScanNode requires a non-nil tableRef.")
	}

	schema := make(LogicalSchema, len(tableRef.schema))
	copy(schema, tableRef.schema)

	n := &LogicalScanNode{
		TableRef:     tableRef,
		outputSchema: schema,
		Predicates:   make([]Expr, 0),
		ForUpdate:    forUpdate,
	}
	n.SetRequiredSchema(n.outputSchema)
	return n
}

func (n *LogicalScanNode) AddPredicate(pred Expr) {
	n.Predicates = append(n.Predicates, pred)
}

func (n *LogicalScanNode) GetTableOid() common.ObjectID {
	if n.TableRef.table == nil {
		panic("LogicalScanNode has a TableRef with no Table! (Is this a subquery ref?)")
	}
	return n.TableRef.table.Oid
}

func (n *LogicalScanNode) GetTableAlias() string {
	return n.TableRef.alias
}

func (n *LogicalScanNode) OutputSchema() LogicalSchema {
	return n.outputSchema
}

func (n *LogicalScanNode) Dependencies() LogicalSchema {
	return make(LogicalSchema, 0)
}

func (n *LogicalScanNode) Children() []LogicalPlanNode {
	return nil
}

func (n *LogicalScanNode) String() string {
	s := fmt.Sprintf("LogicalScan: %s (OID: %d)", n.TableRef.alias, n.GetTableOid())

	if len(n.Predicates) > 0 {
		var preds []string
		for _, p := range n.Predicates {
			preds = append(preds, p.String())
		}
		s += fmt.Sprintf(" | Filter: [%s]", strings.Join(preds, " AND "))
	}
	return s
}
