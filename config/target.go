package config

// Define target
type Target struct {
	SourceTables string `yaml:"source_tables"`
	Endpoint     HTTP   `yaml:"endpoint"`
	ApplySchema  string `yaml:"apply_schema"`
}
