package planner

// LogicalPlanNode represents the static structure of a logical query plan.
type LogicalPlanNode interface {
	// OutputSchema returns the schema of the tuples produced by this node.
	OutputSchema() LogicalSchema

	// Sets the required output schema
	// For projection pushdown
	SetRequiredSchema(schema LogicalSchema)
	GetRequiredSchema() LogicalSchema

	// LogicalColumns required for the node to perform its operation
	Dependencies() LogicalSchema

	// Children returns the child logical plan nodes.
	Children() []LogicalPlanNode

	// String returns a string representation of the logical plan node.
	String() string
}

type BaseLogicalPlanNode struct {
	requiredSchema LogicalSchema
}

func (b *BaseLogicalPlanNode) SetRequiredSchema(s LogicalSchema) {
	b.requiredSchema = s
}

func (b *BaseLogicalPlanNode) GetRequiredSchema() LogicalSchema {
	return b.requiredSchema
}
