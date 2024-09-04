package node

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"

	"github.com/covalenthq/bsp-agent/internal/event"
	"github.com/covalenthq/bsp-agent/internal/handler"
	"github.com/covalenthq/bsp-agent/internal/utils"
	"github.com/go-redis/redis/v7"
)

const (
	consumerEvents            int64  = 1
	consumerPendingIdleTime   int64  = 360
	consumerPendingTimeTicker int64  = 10
	start                     string = ">"
	segmentLength             int    = 1
)

type ethAgentNode struct {
	*agentNode

	lastBlockTime time.Time
}

func newEthAgentNode(anode *agentNode) *ethAgentNode {
	return &ethAgentNode{anode, time.Now()}
}

func (node *ethAgentNode) NodeChainType() ChainType {
	return Ethereum
}

func (node *ethAgentNode) Start(_ context.Context) {
	var consumerName = uuid.NewV4().String()
	log.Printf("Initializing Consumer: %v | Redis Stream: %v | Consumer Group: %v", consumerName, node.streamKey, node.consumerGroup)
	createConsumerGroup(node.RedisClient, node.streamKey, node.consumerGroup)
	node.lastBlockTime = time.Now()
	go node.consumeEvents(consumerName)
	go node.consumePendingEvents(consumerName)
}

func (node *ethAgentNode) consumeEvents(consumerName string) {
	for {
		log.Debug("New sequential stream unit: ", time.Now().Format(time.RFC3339))
		streams, err := node.RedisClient.XReadGroup(&redis.XReadGroupArgs{
			Streams:  []string{node.streamKey, start},
			Group:    node.consumerGroup,
			Consumer: consumerName,
			Count:    consumerEvents,
			Block:    0,
		}).Result()
		if err != nil {
			log.Error("error on consume events: ", err.Error())

			return
		}

		for _, stream := range streams[0].Messages {
			node.eventStreamWaitGrp.Add(1)
			go node.processStream(stream, node.eventStreamWaitGrp)
		}
		node.eventStreamWaitGrp.Wait()
	}
}

// consume pending messages from redis
func (node *ethAgentNode) consumePendingEvents(consumerName string) {
	timeoutDuration := node.AgentConfig.RedisConfig.ConsumerPendingTimeout
	timeout := time.After(time.Second * time.Duration(timeoutDuration))
	ticker := time.NewTicker(time.Second * time.Duration(consumerPendingTimeTicker))
	for {
		select {
		case <-timeout:
			log.Info("Process pending streams stopped at: ", time.Now().Format(time.RFC3339), " after timeout: ", timeoutDuration, " seconds")
			os.Exit(0)
		case <-ticker.C:
			var streamsRetry []string
			pendingStreams, err := node.RedisClient.XPendingExt(&redis.XPendingExtArgs{
				Stream: node.streamKey,
				Group:  node.consumerGroup,
				Start:  "0",
				End:    "+",
				Count:  consumerEvents,
			}).Result()
			if err != nil {
				panic(err)
			}

			for _, stream := range pendingStreams {
				streamsRetry = append(streamsRetry, stream.ID)
			}
			if len(streamsRetry) > 0 {
				streams, err := node.RedisClient.XClaim(&redis.XClaimArgs{
					Stream:   node.streamKey,
					Group:    node.consumerGroup,
					Consumer: consumerName,
					Messages: streamsRetry,
					MinIdle:  time.Duration(consumerPendingIdleTime) * time.Second,
				}).Result()
				if err != nil {
					log.Error("error on process pending: ", err.Error())

					return
				}
				for _, stream := range streams {
					node.pendingEventsWaitGrp.Add(1)
					go node.processStream(stream, node.pendingEventsWaitGrp)
				}
				node.pendingEventsWaitGrp.Wait()
			}
			log.Info("Process pending streams at: ", time.Now().Format(time.RFC3339))
		}
	}
}

func (node *ethAgentNode) processStream(message redis.XMessage, waitGroup *sync.WaitGroup) {
	ctx := context.Background()
	replica, err := handler.ParseMessageToBlockReplica(message)

	if err != nil {
		log.Fatalf("error decoding from redis message: %v", err)
	}
	defer waitGroup.Done()
	objectType := replica.Type()
	objectReplica := replica.Data

	switch {
	case err != nil:
		log.Error("error on process event: ", err)
	case err == nil && objectReplica.Header.Number.Uint64()%uint64(node.AgentConfig.RedisConfig.BlockDivisor) == 0:
		// collect stream ids and block replicas
		segment := &event.ReplicaSegmentWrapped{}
		segment.IDBatch = append(segment.IDBatch, message.ID)
		segment.BlockReplicaEvent = append(segment.BlockReplicaEvent, replica)
		if len(segment.BlockReplicaEvent) == 1 {
			segment.StartBlock = replica.Data.Header.Number.Uint64()
		}
		segment.StartBlock = replica.Data.Header.Number.Uint64()
		segment.EndBlock = replica.Data.Header.Number.Uint64()
		segment.Elements = uint64(segmentLength)
		segment.SegmentName = fmt.Sprint(replica.Data.NetworkId) + "-" + fmt.Sprint(segment.StartBlock) + objectType
		// avro encode, prove and upload

		_, err := node.encodeProveAndUploadReplicaSegment(ctx, segment)
		if err != nil {
			log.Error("failed to avro encode, prove and upload block-result segment with err: ", err)
			panic(err)
		}
		// ack amd trim stream segment batch id
		xlen, err := utils.AckTrimStreamSegment(node.RedisClient, segmentLength, node.streamKey, node.consumerGroup, segment.IDBatch)
		if err != nil {
			log.Error("failed to match streamIDs length to segment length config: ", err)
		}
		log.Info("stream ids acked and trimmed: ", segment.IDBatch, ", for stream key: ", node.streamKey, ", with current length: ", xlen)
		// record metrics
		node.blockProofingMetric.UpdateSince(node.lastBlockTime)
		node.lastBlockTime = time.Now()

	default:
		// collect block replicas and stream ids to skip
		skippedIDs := []string{message.ID}
		log.Info("block-specimen not created for: ", objectReplica.Header.Number.Uint64(), ", base block number divisor is :", node.AgentConfig.RedisConfig.BlockDivisor)
		_, err := utils.AckTrimStreamSegment(node.RedisClient, len(skippedIDs), node.streamKey, node.consumerGroup, skippedIDs)
		if err != nil {
			log.Error("failed to match streamIDs length to segment length config: ", err)
			panic(err)
		}
	}
}

func createConsumerGroup(redisClient *redis.Client, streamKey, consumerGroup string) {
	if _, err := redisClient.XGroupCreateMkStream(streamKey, consumerGroup, "0").Result(); err != nil {
		if !strings.Contains(fmt.Sprint(err), "BUSYGROUP") {
			log.Printf("Error on create Consumer Group: %v ...\n", consumerGroup)
			panic(err)
		}
	}
}

// atomically encodes the event into an AVRO binary, proves the replica on proof-chain and upload and stores the binary file
func (node *ethAgentNode) encodeProveAndUploadReplicaSegment(ctx context.Context, currentSegment *event.ReplicaSegmentWrapped) (string, error) {
	replicaSegmentAvro, err := handler.EncodeReplicaSegmentToAvro(node.ReplicaCodec, currentSegment.ReplicationSegment)
	if err != nil {
		return "", fmt.Errorf("error encoding to avro: %w", err)
	}
	log.Infof("\n---> Processing %s <---\n", currentSegment.SegmentName)

	replicaURL, ccid := node.StorageManager.GenerateLocation(currentSegment.SegmentName, replicaSegmentAvro)
	log.Info("eth binary file should be available: ", replicaURL)

	log.Info("submitting block-replica segment proof for: ", currentSegment.SegmentName)
	proofTxHash := make(chan string, 1)
	lastBlockReplica := currentSegment.BlockReplicaEvent[len(currentSegment.BlockReplicaEvent)-1]
	// go node.proofchi.SendBlockReplicaProofTx(ctx, currentSegment.EndBlock, lastBlockReplica.Data, replicaSegmentAvro, replicaURL, proofTxHash)
	go node.covenet.SendCovenetBlockReplicaProofTx(ctx, currentSegment.EndBlock, lastBlockReplica.Data, replicaSegmentAvro, replicaURL, proofTxHash)
	pTxHash := <-proofTxHash

	switch {
	case strings.Contains(pTxHash, "session closed"):
		return pTxHash, nil
	case strings.Contains(pTxHash, "presubmitted hash"):
		return pTxHash, nil
	case strings.Contains(pTxHash, "mine timeout"):
		return pTxHash, nil
	case strings.Contains(pTxHash, "retry fail"):
		return pTxHash, nil
	case strings.Contains(pTxHash, "out-of-bounds block"):
		return pTxHash, nil
	case strings.Contains(pTxHash, "invalid block"):
		return pTxHash, nil
	case strings.Contains(pTxHash, "already known"):
		return pTxHash, nil
	case strings.Contains(pTxHash, "max submissions limit exceeded"):
		return pTxHash, nil
	case pTxHash == "":
		return "", fmt.Errorf("failed to prove & upload block-replica segment event: %v", currentSegment.SegmentName)
	default:
		// success. Store now...
		log.Info("Proof-chain tx hash: ", pTxHash, " for block-replica segment: ", currentSegment.SegmentName)
		filename := objectFileName(currentSegment.SegmentName, pTxHash)
		err = node.StorageManager.Store(ccid, filename, replicaSegmentAvro)

		if err != nil {
			return "", fmt.Errorf("error in storing object: %w", err)
		}
	}

	return pTxHash, nil
}

func objectFileName(objectName, txHash string) string {
	return objectName + "-" + txHash
}
