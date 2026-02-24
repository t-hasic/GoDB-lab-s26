package planner

import (
	"fmt"

	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
)

type ExpressionBinder struct{}

func NewExpressionBinder() *ExpressionBinder {
	return &ExpressionBinder{}
}

// BindExpr converts LogicalColumn pointers into BoundValueExpr (calculating integer offsets)
// The bindExpr function in logical plan builder only builds LOGICAL expression trees.
func (b *ExpressionBinder) BindExpr(e Expr, logicalSchema LogicalSchema, physicalSchema []common.Type) Expr {
	switch v := e.(type) {
	case *LogicalColumn:
		for i, col := range logicalSchema {
			if col.Equals(v) {
				// We use the index 'i' to find the type in the physical schema
				return NewColumnValueExpression(i, physicalSchema, v.cname)
			}
		}
		panic(fmt.Sprintf("ExpressionBinder Failed: Column %s not found in input logicalSchema, %v", v.cname, logicalSchema))

	case *ComparisonExpression:
		return NewComparisonExpression(
			b.BindExpr(v.left, logicalSchema, physicalSchema),
			b.BindExpr(v.right, logicalSchema, physicalSchema),
			v.compType,
		)
	case *BinaryLogicExpression:
		return NewBinaryLogicExpression(
			b.BindExpr(v.left, logicalSchema, physicalSchema),
			b.BindExpr(v.right, logicalSchema, physicalSchema),
			v.logicType,
		)
	case *ArithmeticExpression:
		return NewArithmeticExpression(
			b.BindExpr(v.left, logicalSchema, physicalSchema),
			b.BindExpr(v.right, logicalSchema, physicalSchema),
			v.op,
		)
	case *NegationExpression:
		return NewNegationExpression(
			b.BindExpr(v.child, logicalSchema, physicalSchema),
		)
	case *NullCheckExpression:
		return NewNullCheckExpression(
			b.BindExpr(v.child, logicalSchema, physicalSchema),
			v.checkType,
		)
	case *StringConcatExpression:
		return NewStringConcatenation(
			b.BindExpr(v.left, logicalSchema, physicalSchema),
			b.BindExpr(v.right, logicalSchema, physicalSchema),
		)
	case *LikeExpression:
		return NewLikeExpression(
			b.BindExpr(v.left, logicalSchema, physicalSchema),
			b.BindExpr(v.right, logicalSchema, physicalSchema),
		)
	case *ConstantValueExpr:
		return v
	}
	return e
}

type PhysicalPlanBuilder struct {
	catalog    *catalog.Catalog
	rules      []PhysicalConversionRule
	exprBinder *ExpressionBinder
}

func NewPhysicalPlanBuilder(catalog *catalog.Catalog, customRules []PhysicalConversionRule) *PhysicalPlanBuilder {
	return &PhysicalPlanBuilder{
		catalog:    catalog,
		rules:      customRules,
		exprBinder: NewExpressionBinder(),
	}
}

func (b *PhysicalPlanBuilder) Build(logicalPlan LogicalPlanNode) (PlanNode, error) {
	logicalChildren := logicalPlan.Children()
	physicalChildren := make([]PlanNode, len(logicalChildren))

	for i, child := range logicalChildren {
		physChild, err := b.Build(child)
		if err != nil {
			return nil, err
		}
		physicalChildren[i] = physChild
	}

	var bestRule PhysicalConversionRule
	maxPriority := -1
	for _, rule := range b.rules {
		if rule.Match(logicalPlan, physicalChildren, b.catalog) && rule.Priority() > maxPriority {
			bestRule = rule
			maxPriority = rule.Priority()
		}
	}

	if bestRule == nil {
		return nil, fmt.Errorf("No physical conversion rule matched for node: %s", logicalPlan.String())
	}

	return bestRule.Apply(logicalPlan, physicalChildren, b.catalog, b.exprBinder)
}
