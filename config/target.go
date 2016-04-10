package config

// Define target
type Target struct {
	TargetExpression string `yaml:"target_expression"`
	Endpoint         HTTP   `yaml:"endpoint"`
	ApplySchema      string `yaml:"apply_schema"`
}
