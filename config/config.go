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
	UseEventTriggers    bool                `yaml:"use_event_triggers"`
}

func (c Config) InvalidProcessingIntervals() bool {
	// Some processing intervals only make sense when there are actually
	// targets to send data to.
	if len(c.Targets) > 0 {
		if c.ProcessingIntervals.Batcher <= 0 {
			return true
		}

		if c.ProcessingIntervals.Transmitter <= 0 {
			return true
		}

		if !c.UseEventTriggers && c.ProcessingIntervals.DdlWatcher <= 0 {
			return true
		}
	}

	if c.ProcessingIntervals.Applier <= 0 {
		return true
	}

	if c.ProcessingIntervals.Vacuum <= 0 {
		return true
	}

	return false
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
