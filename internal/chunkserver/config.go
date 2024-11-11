package chunkserver

import (
    "fmt"
    "os"
    
    "gopkg.in/yaml.v2"
)

type Config struct {
    Server struct {
        MasterAddress     string        `yaml:"master_address"`
        DataDir          string        `yaml:"data_dir"`
        HeartbeatInterval int           `yaml:"heartbeat_interval"`
    } `yaml:"server"`
    
    Storage struct {
        MaxChunkSize   int64         `yaml:"max_chunk_size"`
        BufferSize     int           `yaml:"buffer_size"`
        FlushInterval  int           `yaml:"flush_interval"`
    } `yaml:"storage"`
    
    Operation struct {
        ReadTimeout    int           `yaml:"read_timeout"`
        WriteTimeout   int           `yaml:"write_timeout"`
        RetryAttempts  int           `yaml:"retry_attempts"`
        RetryDelay     int           `yaml:"retry_delay"`
    } `yaml:"operation"`
}

func LoadConfig(path string) (*Config, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, fmt.Errorf("error reading config file: %v", err)
    }
    
    config := &Config{}
    if err := yaml.Unmarshal(data, config); err != nil {
        return nil, fmt.Errorf("error parsing config file: %v", err)
    }
    
    return config, nil
}