// Package node contains structs/interfaces for bsp-agent node, it aggregates all the services and orchestrates processing of redis stream (or websocket) encoded (rlp for evm) block-replica  messages created by bsp-geth
package node

import (
	"context"
	"sync"

	log "github.com/sirupsen/logrus"
	"gopkg.in/avro.v0"

	"github.com/covalenthq/bsp-agent/internal/config"
	"github.com/covalenthq/bsp-agent/internal/event"
	"github.com/covalenthq/bsp-agent/internal/ewm"
	"github.com/covalenthq/bsp-agent/internal/metrics"
	"github.com/covalenthq/bsp-agent/internal/proof"
	"github.com/covalenthq/bsp-agent/internal/storage"
	"github.com/covalenthq/bsp-agent/internal/utils"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/go-redis/redis/v7"
	"github.com/linkedin/goavro/v2"
)

// AgentNode defines the interface to interact with the bsp-agent node.
// The lifecycle of an AgentNode goes from:
// start (processing) -> stop (processing) -> close.
type AgentNode interface {
	NodeChainType() ChainType
	Start(_ context.Context)
	StopProcessing()
	Close()
}

// ChainType ChainType is the blockchain on which the agent is configured to run.
// Current allowed values are "ethereum" and "elrond"
type ChainType string

const (
	// Ethereum ChainType
	Ethereum ChainType = "ethereum"
	// Elrond ChainType
	Elrond ChainType = "elrond"
)

type agentNode struct {
	AgentConfig *config.AgentConfig

	RedisClient *redis.Client

	// storage
	StorageManager *storage.Manager

	// codec
	ReplicaCodec *goavro.Codec
	EthClient    *ethclient.Client

	eventStreamWaitGrp   *sync.WaitGroup
	pendingEventsWaitGrp *sync.WaitGroup

	// proochain
	proofchi *proof.ProofchainInteractor

	// covenet
	covenet *ewm.CovenetInteractor

	// redis derived settings
	streamKey     string
	consumerGroup string

	// stream processing
	//nolint // false positive in structcheck linter causes it to incorrectly identify `segment` as unused
	segment event.ReplicaSegmentWrapped

	// metrics
	blockProofingMetric metrics.Timer // captures duration and rate of block proofing
}

// NewAgentNode creates a new agent node of given ChainType, and config.
// This also sets up the internal services in order for the node to operate.
// Typically one can `Start()` processing after creating node via this method.
func NewAgentNode(chainType ChainType, aconfig *config.AgentConfig) AgentNode {
	anode := &agentNode{}
	anode.AgentConfig = aconfig
	anode.setupRedis()
	anode.setupEthClient()
	anode.setupReplicaCodec()
	anode.setupStorageManager()
	anode.setupProofchainInteractor()
	anode.setupWaitGrps()
	anode.setupMetrics()
	anode.setupCovenetInteractor()

	switch chainType {
	case Ethereum:
		return newEthAgentNode(anode)
	case Elrond:
		return newElrondAgentNode(anode)
	default:
		log.Fatalf("unknown chainType requested: %s", chainType)
	}

	// unreachable
	return nil
}

func (anode *agentNode) NodeChainType() ChainType {
	log.Fatal("NodeChainType() shouldn't be called directly for AgentNode")
	// unreachable
	return ""
}

func (anode *agentNode) Start(_ context.Context) {
	log.Fatal("Start() shouldn't be called directly for AgentNode")
}

func (anode *agentNode) StopProcessing() {
	log.Warn("Received interrupt. Flushing in-memory blocks...")
	anode.eventStreamWaitGrp.Wait()
	anode.pendingEventsWaitGrp.Wait()
	log.Warn("waitgrp ended. Closing node...")
}

func (anode *agentNode) Close() {
	if anode.RedisClient != nil {
		err := anode.RedisClient.Close()
		if err != nil {
			log.Error("error in closing redis client: ", err)
		}
	}

	anode.StorageManager.Close()
	anode.EthClient.Close()
}

func (anode *agentNode) setupRedis() {
	redisClient, streamKey, consumerGroup, err := utils.NewRedisClient(&anode.AgentConfig.RedisConfig)
	if err != nil {
		log.Fatalf("unable to get redis client from redis URL flag: %v", err)
	}

	// setup redis client
	anode.RedisClient = redisClient
	anode.streamKey = streamKey
	anode.consumerGroup = consumerGroup
}

func (anode *agentNode) setupEthClient() {
	ethClient, err := utils.NewEthClient(anode.AgentConfig.ChainConfig.RPCURL)
	if err != nil {
		log.Fatalf("unable to get ethereum client from Eth client flag: %v", err)
	}

	anode.EthClient = ethClient
}

func (anode *agentNode) setupReplicaCodec() {
	replicaAvro, err := avro.ParseSchemaFile(anode.AgentConfig.CodecConfig.AvroCodecPath)
	if err != nil {
		log.Fatalf("unable to parse avro schema for specimen from codec path flag: %v", err)
	}
	replicaCodec, err := goavro.NewCodec(replicaAvro.String())
	if err != nil {
		log.Fatalf("unable to generate avro codec for block-replica: %v", err)
	}

	anode.ReplicaCodec = replicaCodec
}

func (anode *agentNode) setupStorageManager() {
	storageManager, err := storage.NewStorageManager(&anode.AgentConfig.StorageConfig)
	if err != nil {
		log.Fatalf("unable to setup storage manager: %v", err)
	}

	anode.StorageManager = storageManager
}

func (anode *agentNode) setupProofchainInteractor() {
	anode.proofchi = proof.NewProofchainInteractor(anode.AgentConfig, anode.EthClient)
}

func (anode *agentNode) setupWaitGrps() {
	anode.eventStreamWaitGrp = new(sync.WaitGroup)
	anode.pendingEventsWaitGrp = new(sync.WaitGroup)
}

func (anode *agentNode) setupMetrics() {
	anode.blockProofingMetric = metrics.GetOrRegisterTimer("agent/blocks/success", metrics.DefaultRegistry)
}

func (anode *agentNode) setupCovenetInteractor() {
	covenetInteractor, err := ewm.NewCovenetInteractor(anode.AgentConfig)
	if err != nil {
		log.Fatalf("unable to setup covenet interactor: %v", err)
	}
	anode.covenet = covenetInteractor
}
