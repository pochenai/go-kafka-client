package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	Brokers   []string `toml:"brokers"`
	Topic     string   `toml:"topic"`
	GroupID   string   `toml:"group_id"`
	TargetTag int      `toml:"target_tag"`
	StoreFile string   `toml:"store_file"`
}

// eventEnvelope is the outermost wrapper of the Kafka message; the real token
// structure lives in the data field.
type eventEnvelope struct {
	Topic  string     `json:"topic"`
	Source *string    `json:"source"`
	Type   *string    `json:"type"`
	Data   tokenEvent `json:"data"`
}

// tokenLogicID is the chain index + token address that guarantees logical uniqueness.
type tokenLogicID struct {
	ChainIndex   int    `json:"chainIndex"`
	TokenAddress string `json:"tokenAddress"`
}

// tokenEvent 对应文档 gasless增量更新.md 中 data 字段的完整结构。
// tags 是 code 集合，RWA_XSTOCK = 36。可空字段用指针表达 null/缺省。
type tokenEvent struct {
	TokenLogicID           tokenLogicID    `json:"tokenLogicId"`
	Name                   *string         `json:"name"`                   // 脱敏后的名字
	Symbol                 *string         `json:"symbol"`                 // 脱敏后的符号
	Decimal                *int            `json:"decimal"`                // 精度，如 18
	CustomName             *string         `json:"customName"`             // 业务线展示用自定义名字
	CustomSymbol           *string         `json:"customSymbol"`           // 业务线展示用自定义符号
	LogoUrl                *string         `json:"logoUrl"`                // Logo URL
	CoinTypeNo             *int64          `json:"coinTypeNo"`             // 兼容钱包逻辑
	CreatedTime            int64           `json:"createdTime"`            // Token 创建时间(ms)
	UpdatedTime            int64           `json:"updatedTime"`            // Token 更新时间(ms)
	EventTime              int64           `json:"eventTime"`              // 事件生产时间(ms)，用于发现消费积压
	Protocol               *string         `json:"protocol"`               // NFT 协议 / spl-token 等
	Tags                   []int           `json:"tags"`                   // tag code 集合
	Native                 *bool           `json:"native"`                 // 本链原生代币
	TokenID                *string         `json:"tokenId"`                // 链上唯一 id
	Rank                   *int            `json:"rank"`                   // 排名
	Source                 *string         `json:"source"`                 // 同步来源，如 "vault_add"
	IsDeleted              *bool           `json:"isDeleted"`              // 是否已删除
	TokenCreatorAddress    *string         `json:"tokenCreatorAddress"`    // 创建者地址
	TokenOnChainCreateTime *int64          `json:"tokenOnChainCreateTime"` // 链上创建时间(ms)
	LogoUrlLarge           *string         `json:"logoUrlLarge"`           // Logo 大图
	RawName                *string         `json:"rawName"`                // 链上原始 name
	RawSymbol              *string         `json:"rawSymbol"`              // 链上原始 symbol
	LogoPass               *bool           `json:"logoPass"`               // logo 是否过审
	LatestMultiplier       json.RawMessage `json:"latestMultiplier"`       // RWA Scaled UI multiplier，可能是字符串或数字
	MultiplierUpdatedAt    *int64          `json:"multiplierUpdatedAt"`    // multiplier 更新时间(ms)
	IsRwa                  *bool           `json:"isRwa"`                  // 是否 RWA
	ID                     string          `json:"id"`                     // chainIndex-tokenAddress
}

// storedEvent is one record persisted to the local JSONL file, wrapping the raw
// message with metadata for later querying/dedup.
type storedEvent struct {
	ReceivedAt time.Time       `json:"received_at"`
	Partition  int             `json:"partition"`
	Offset     int64           `json:"offset"`
	KafkaTime  time.Time       `json:"kafka_time"`
	Raw        json.RawMessage `json:"raw"`
}

func main() {
	cfgPath := "config.toml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	var cfg Config
	if _, err := toml.DecodeFile(cfgPath, &cfg); err != nil {
		log.Fatalf("failed to load config %s: %v", cfgPath, err)
	}

	// Open the local store file append-only, single writer appending.
	store, err := os.OpenFile(cfg.StoreFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("failed to open store file %s: %v", cfg.StoreFile, err)
	}
	defer store.Close()
	enc := json.NewEncoder(store) // each Encode appends a newline, naturally JSONL

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		StartOffset: kafka.FirstOffset, // only applies when the group has no committed offset (first run starts from earliest, then resumes)
	})
	defer reader.Close()

	// Graceful shutdown on Ctrl-C / SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Probe broker connectivity before consuming, then log the reader status.
	if err := probeBrokers(ctx, cfg.Brokers); err != nil {
		log.Fatalf("failed to connect to brokers %v: %v", cfg.Brokers, err)
	}
	st := reader.Stats()
	log.Printf("kafka reader ready clientID=%q topic=%s partition=%s offset=%d lag=%d queueLen=%d/%d",
		st.ClientID, st.Topic, st.Partition, st.Offset, st.Lag, st.QueueLength, st.QueueCapacity)

	log.Printf("consuming topic=%s group=%s, filtering tag=%d, storing to %s ...",
		cfg.Topic, cfg.GroupID, cfg.TargetTag, cfg.StoreFile)

	var hits int
	for {
		msg, err := reader.ReadMessage(ctx) // auto-commits offset
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("received shutdown signal, stopped. matched %d RWA_XSTOCK events this run", hits)
				return
			}
			log.Printf("error reading message: %v", err)
			continue
		}
		var env eventEnvelope
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			log.Printf("skipping unparseable message (offset=%d): %v", msg.Offset, err)
			continue
		}
		ev := env.Data

		if !slices.Contains(ev.Tags, cfg.TargetTag) {
			continue // not RWA_XSTOCK, skip
		}

		rec := storedEvent{
			ReceivedAt: time.Now(),
			Partition:  msg.Partition,
			Offset:     msg.Offset,
			KafkaTime:  msg.Time,
			Raw:        json.RawMessage(msg.Value),
		}
		if err := enc.Encode(&rec); err != nil {
			log.Printf("failed to write to store (offset=%d): %v", msg.Offset, err)
			continue
		}

		hits++
		log.Printf("[RWA_XSTOCK] saved #%d  symbol=%s id=%s tags=%v isRwa=%v  (partition=%d offset=%d)",
			hits, derefStr(ev.Symbol), ev.ID, ev.Tags, derefBool(ev.IsRwa), msg.Partition, msg.Offset)
	}
}

// probeBrokers dials the configured brokers to verify connectivity and logs the
// cluster info reported by the first reachable one. Returns the last error if
// none can be reached.
func probeBrokers(ctx context.Context, brokers []string) error {
	var lastErr error
	for _, addr := range brokers {
		conn, err := kafka.DialContext(ctx, "tcp", addr)
		if err != nil {
			lastErr = err
			continue
		}
		bs, err := conn.Brokers()
		if err != nil {
			conn.Close()
			lastErr = err
			continue
		}
		controller, _ := conn.Controller()
		conn.Close()
		log.Printf("kafka connected to broker %s, cluster has %d brokers, controller=%s:%d",
			addr, len(bs), controller.Host, controller.Port)
		return nil
	}
	return lastErr
}

// derefStr safely dereferences a nullable string field, showing "<nil>" for nil.
func derefStr(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

// derefBool safely dereferences a nullable bool field, treating nil as false.
func derefBool(b *bool) bool {
	return b != nil && *b
}
