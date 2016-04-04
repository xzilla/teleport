package config

// Define HTTP server config
type HTTP struct {
	Hostname string `yaml:"hostname"`
	Port     int    `yaml:"port"`
}

