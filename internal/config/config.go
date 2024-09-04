// Package config contains all the config functions that is used by the bsp-agent node
package config

import (
	"flag"
)

var (
	blockDivisorDefault           = 3
	consumerPendingTimeoutDefault = 180
	logFolderDefault              = "./logs/"
)

// RedisConfig contains all redis related config
type RedisConfig struct {
	RedisURL               string
	Password               string
	BlockDivisor           int // can be set to any divisor (decrease specimen production throughput)
	ConsumerPendingTimeout int // defaults to 1 min
}

// CodecConfig contains all AVRO codec related config
type CodecConfig struct {
	AvroCodecPath string
}

// StorageConfig contains all configs needed by different stores
type StorageConfig struct {
	// local
	BinaryFilePath string

	// ipfs
	IpfsPinnerServer string
}

// ProofchainConfig contains all proof-chain configs
type ProofchainConfig struct {
	ProofChainAddr string
}

// ChainConfig contains config for all supported blockchains
type ChainConfig struct {
	RPCURL       string
	PrivateKey   string
	KeystorePath string
	KeyStorePwd  string

	// for elrond
	WebsocketURLs string
}

// CovenetConfig contains the gRPC url and private key for EWM node
type CovenetConfig struct {
	PrivateKey string
	GRPCURL    string
}

// MetricsConfig contains config for collecting performance metrics
type MetricsConfig struct {
	Enabled        bool
	HTTPServerAddr string
	HTTPServerPort string
}

// AgentConfig composes all configs into a single full (env and flags) config for the bsp-agent node
type AgentConfig struct {
	RedisConfig      RedisConfig
	CodecConfig      CodecConfig
	StorageConfig    StorageConfig
	ProofchainConfig ProofchainConfig
	ChainConfig      ChainConfig
	MetricsConfig    MetricsConfig
	CovenetConfig    CovenetConfig

	LogFolder string
}

// NewAgentConfig creates a new empty config
func NewAgentConfig() *AgentConfig {
	return &AgentConfig{}
}

// LoadConfig gets the config from env flags and cli arguments
func (ac *AgentConfig) LoadConfig() {
	envConfig, err := loadEnvConfig()
	if err != nil {
		panic(err)
	}

	ac.getConfigFromEnv(envConfig)
	ac.getConfigFromFlags()
}

// SegmentLength get number of block-specimens encoded within a block-replica
func (ac *AgentConfig) SegmentLength() int {
	// number of block specimen/results within a single uploaded avro encoded block-replica object
	// defaults to 1 block per segment in bsp-geth/agent "live" mode
	return 1
}

func (ac *AgentConfig) getConfigFromEnv(config *EnvConfig) {
	ac.StorageConfig.IpfsPinnerServer = config.IpfsConfig.IpfsPinnerServer

	ac.RedisConfig.Password = config.RedisConfig.Password

	ac.ChainConfig.RPCURL = config.EthConfig.RPCURL
	ac.ChainConfig.PrivateKey = config.EthConfig.PrivateKey
	ac.ChainConfig.KeystorePath = config.EthConfig.KeystorePath
	ac.ChainConfig.KeyStorePwd = config.EthConfig.KeyStorePwd

	ac.CovenetConfig.GRPCURL = config.CovenetConfig.GRPCURL
	ac.CovenetConfig.PrivateKey = config.CovenetConfig.PrivateKey
}

func (ac *AgentConfig) getConfigFromFlags() {
	// redis
	flag.StringVar(&ac.RedisConfig.RedisURL, "redis-url", LookupEnvOrString("RedisURL", ""), "redis consumer stream url")
	flag.IntVar(&ac.RedisConfig.BlockDivisor, "block-divisor", LookupEnvOrInt("BlockDivisor", blockDivisorDefault), "integer divisor that allows for selecting only block numbers divisible by this number")
	flag.IntVar(&ac.RedisConfig.ConsumerPendingTimeout, "consumer-timeout", LookupEnvOrInt("ConsumerPendingTimeout", consumerPendingTimeoutDefault), "number of seconds to wait before pending messages consumer timeout")

	// avro codec
	flag.StringVar(&ac.CodecConfig.AvroCodecPath, "avro-codec-path", LookupEnvOrString("CodecPath", ""), "local path to AVRO .avsc files housing the specimen/result schemas")

	// proof-chain
	flag.StringVar(&ac.ProofchainConfig.ProofChainAddr, "proof-chain-address", LookupEnvOrString("ProofChain", ""), "hex string address for deployed proof-chain contract")
	flag.StringVar(&ac.ChainConfig.WebsocketURLs, "websocket-urls", LookupEnvOrString("WebsocketURLs", ""), "url to websockets clients separated by space")

	// logs
	flag.StringVar(&ac.LogFolder, "log-folder", LookupEnvOrString("LogFolder", logFolderDefault), "Location where the log files should be placed")

	// storage
	flag.StringVar(&ac.StorageConfig.IpfsPinnerServer, "ipfs-pinner-server", LookupEnvOrString("IpfsPinnerServer", "http://127.0.0.1:3001/"), "IPFS pinner server url for uploading data")
	flag.StringVar(&ac.StorageConfig.BinaryFilePath, "binary-file-path", LookupEnvOrString("BinaryFilePath", ""), "local path to AVRO encoded binary files that contain block-replicas")

	// metrics
	flag.BoolVar(&ac.MetricsConfig.Enabled, "metrics", false, "enable metrics reporting and collection")
	flag.StringVar(&ac.MetricsConfig.HTTPServerAddr, "metrics.addr", LookupEnvOrString("MetricsHttpServerAddr", "127.0.0.1"), "Enable stand-alone metrics HTTP server listening interface (default: \"127.0.0.1\")")
	flag.StringVar(&ac.MetricsConfig.HTTPServerPort, "metrics.port", LookupEnvOrString("MetricsHttpServerPort", "6061"), "Metrics HTTP server listening port (default: 6061)")

	flag.Parse()
}
