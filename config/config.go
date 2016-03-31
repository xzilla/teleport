package config

import (
	"github.com/go-yaml/yaml"
	"github.com/pagarme/teleport/database"
	"io/ioutil"
)

// Define HTTP server config
type HTTP struct {
	Hostname string `yaml:"hostname"`
	Port     int    `yaml:"port"`
}

// Define target
type Target struct {
	SourceTables string `yaml:"source_tables"`
	Endpoint     HTTP   `yaml:"endpoint"`
	ApplySchema  string `yaml:"apply_schema"`
}

type Config struct {
	Database database.Database `yaml:"database"`
	HTTP     HTTP              `yaml:"http"`
	Targets  []Target          `yaml:"targets"`
}

func New() *Config {
	return &Config{}
}

// Open YAML file from path and unmarshal its content
// inside Config object itself
func (c *Config) ReadFromFile(path string) error {
	b, err := ioutil.ReadFile(path)

	if err != nil {
		return err
	}

	if err := yaml.Unmarshal(b, c); err != nil {
		return err
	}

	return nil
}
