package config

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

// RedisEnvConfig contains all config for redis
type RedisEnvConfig struct {
	Password string `envconfig:"REDIS_PWD" default:""`
}

// CovenetEnvConfig (ewm) node config
type CovenetEnvConfig struct {
	PrivateKey string `envconfig:"COVENET_PRIVATE_KEY"`
	GRPCURL    string `envconfig:"COVENET_GRPC_URL"`
}

// EthEnvConfig contains all config for ethereum / ethereum like (EVM) networks
type EthEnvConfig struct {
	RPCURL       string `envconfig:"MB_RPC_URL"`
	PrivateKey   string `envconfig:"MB_PRIVATE_KEY"`
	KeystorePath string `envconfig:"MB_KEYSTORE_PATH"`
	KeyStorePwd  string `envconfig:"MB_KEYSTORE_PWD"`
}

// IpfsEnvConfig contains all config for IPFS pinning services
type IpfsEnvConfig struct {
	IpfsPinnerServer string `envconfig:"IFPS_PINNER_SERVER" default:"http://127.0.0.1:3000"`
}

// EnvConfig composes all configs into a single env config for the bsp-agent node
type EnvConfig struct {
	IpfsConfig    IpfsEnvConfig
	RedisConfig   RedisEnvConfig
	EthConfig     EthEnvConfig
	CovenetConfig CovenetEnvConfig
}

func loadEnvConfig() (*EnvConfig, error) {
	cfg := &EnvConfig{}
	err := envconfig.Process("", cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to parse configuration: %w", err)
	}

	return cfg, nil
}
