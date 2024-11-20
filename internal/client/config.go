package client

import (
	"fmt"
	"io/ioutil"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Connection struct {
		Master struct {
			Host    string `yaml:"host"`
			Port    int    `yaml:"port"`
			Timeout int    `yaml:"timeout"`
		} `yaml:"master"`
		MaxRetries     int `yaml:"max_retries"`
		RetryInterval  int `yaml:"retry_interval"`
		RequestTimeout int `yaml:"request_timeout"`
	} `yaml:"connection"`

	Cache struct {
		ChunkLocation struct {
			Enabled bool `yaml:"enabled"`
			Size    int  `yaml:"size"`
			TTL     int  `yaml:"ttl"`
		} `yaml:"chunk_location"`
		Metadata struct {
			Enabled bool `yaml:"enabled"`
			Size    int  `yaml:"size"`
			TTL     int  `yaml:"ttl"`
		} `yaml:"metadata"`
	} `yaml:"cache"`

	Operation struct {
		Chunk struct {
			ReadSize     int64 `yaml:"read_size"`
			WriteSize    int64 `yaml:"write_size"`
			VerifyWrites bool  `yaml:"verify_writes"`
		} `yaml:"chunk"`
		Retries struct {
			MaxAttempts int `yaml:"max_attempts"`
			BackoffBase int `yaml:"backoff_base"`
		} `yaml:"retries"`
		Timeouts struct {
			Read   int `yaml:"read"`
			Write  int `yaml:"write"`
			Delete int `yaml:"delete"`
		} `yaml:"timeouts"`
	} `yaml:"operation"`

	Logging struct {
		Level     string `yaml:"level"`
		Format    string `yaml:"format"`
		Directory string `yaml:"directory"`
		MaxSize   int    `yaml:"max_size"`
		MaxFiles  int    `yaml:"max_files"`
	} `yaml:"logging"`

	Monitoring struct {
		Enabled        bool `yaml:"enabled"`
		UpdateInterval int  `yaml:"update_interval"`
		MetricsPort    int  `yaml:"metrics_port"`
	} `yaml:"monitoring"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %v", err)
	}

	config := &Config{}
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("error parsing config file: %v", err)
	}

	return config, nil
}

func (c *Config) ToClientConfig() *ClientConfig {
	return &ClientConfig{
		MasterAddr: fmt.Sprintf("%s:%d", c.Connection.Master.Host, c.Connection.Master.Port),
		RetryPolicy: RetryConfig{
			MaxAttempts: c.Connection.MaxRetries,
			Interval:    time.Duration(c.Connection.RetryInterval) * time.Millisecond,
		},
		Cache: CacheConfig{
			ChunkTTL: time.Duration(c.Cache.ChunkLocation.TTL) * time.Second,
		},
		Timeouts: TimeoutConfig{
			Operation:  time.Duration(c.Operation.Timeouts.Write) * time.Second,
			Connection: time.Duration(c.Connection.Master.Timeout) * time.Second,
		},
	}
}
