package config

import (
	"github.com/go-yaml/yaml"
	"io/ioutil"
)

type ProcessingIntervals struct {
	Batcher     int `yaml:"batcher"`
	Transmitter int `yaml:"transmitter"`
	Applier     int `yaml:"applier"`
	Vacuum      int `yaml:"vacuum"`
	DdlWatcher  int `yaml:"ddlwatcher"`
}

type Config struct {
	SentryEndpoint      string              `yaml:"sentry_endpoint"`
	Database            Database            `yaml:"database"`
	ServerHTTP          HTTP                `yaml:"server"`
	Targets             map[string]Target   `yaml:"targets"`
	BatchSize           int                 `yaml:"batch_size"`
	ProcessingIntervals ProcessingIntervals `yaml:"processing_intervals"`
	MaxEventsPerBatch   int                 `yaml:"max_events_per_batch"`
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
