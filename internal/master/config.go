package master

import (
    "fmt"
    "io/ioutil"
    "gopkg.in/yaml.v3"
)

type Config struct {
    Chunk struct {
        Size             int64  `yaml:"size"`
        NamingPattern    string `yaml:"naming_pattern"`
        ChecksumAlgorithm string `yaml:"checksum_algorithm"`
        VerifyOnRead     bool   `yaml:"verify_on_read"`
    } `yaml:"chunk"`

    Deletion struct {
        GCInterval      int   `yaml:"g_c_interval"`
        RetentionPeriod     int   `yaml:"retention_period"`
        GCDeleteBatchSize     int   `yaml:"g_c_delete_batch_size"`
        TrashDirPrefix     string   `yaml:"trash_dir_prefix"`
    } `yaml:"deletion"`
    
    Health struct {
        CheckInterval  int `yaml:"check_interval"`
        Timeout       int `yaml:"timeout"`
        MaxFailures   int `yaml:"max_failures"`
    } `yaml:"health"`
    
    Metadata struct {
        Database struct {
            Type           string `yaml:"type"`
            Path           string `yaml:"path"`
            BackupInterval int    `yaml:"backup_interval"`
        } `yaml:"database"`
        MaxFilenameLength int `yaml:"max_filename_length"`
        MaxDirectoryDepth int `yaml:"max_directory_depth"`
        CacheSizeMB      int `yaml:"cache_size_mb"`
        CacheTTLSeconds  int `yaml:"cache_ttl_seconds"`
    } `yaml:"metadata"`

    Lease struct {
        LeaseTimeout     int `yaml:"lease_timeout"`
    } `yaml:"lease"`

    OperationLog struct {
        Path           string `yaml:"path"`
    } `yaml:"operation_log"`

    Replication struct {
        Factor      int   `yaml:"factor"`
        Timeout     int   `yaml:"timeout"`
    } `yaml:"replication"`

    Server struct {
        Host            string `yaml:"host"`
        Port            int    `yaml:"port"`
        MaxConnections  int    `yaml:"max_connections"`
        ConnectionTimeout int  `yaml:"connection_timeout"`
        MaxRequestSize  int64  `yaml:"max_request_size"`
        ThreadPoolSize  int    `yaml:"thread_pool_size"`
    } `yaml:"server"`
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
