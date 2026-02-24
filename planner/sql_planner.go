package planner

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
	"mit.edu/dsg/godb/catalog"
)

// SQLPlanner orchestrates the parsing, logical planning, optimization,
// and physical planning steps.
type SQLPlanner struct {
	lb  *LogicalPlanBuilder
	opt *Optimizer
	pb  *PhysicalPlanBuilder
}

func NewSQLPlanner(c *catalog.Catalog, logicalRules []LogicalRule, physicalRules []PhysicalConversionRule) *SQLPlanner {
	return &SQLPlanner{
		lb:  NewLogicalPlanBuilder(c),
		opt: NewOptimizer(logicalRules),
		pb:  NewPhysicalPlanBuilder(c, physicalRules),
	}
}

// Plan takes a SQL string and returns a constructed Physical Plan tree.
// It handles the pipeline: SQL -> AST -> Logical -> Optimized -> Physical.
func (p *SQLPlanner) Plan(sql string) (PlanNode, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	logicalPlan, err := p.lb.Plan(stmt)
	if err != nil {
		return nil, fmt.Errorf("logical planning error: %w", err)
	}

	optimizedPlan := p.opt.Optimize(logicalPlan)

	physicalPlan, err := p.pb.Build(optimizedPlan)
	if err != nil {
		return nil, fmt.Errorf("physical planning error: %w", err)
	}

	return physicalPlan, nil
}
