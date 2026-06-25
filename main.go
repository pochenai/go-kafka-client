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

// 只解析过滤/摘要需要的字段；完整消息体原样存进 raw。
// tags 是 code 集合，RWA_XSTOCK = 36。
type tokenEvent struct {
	ID     string `json:"id"`
	Symbol string `json:"symbol"`
	Tags   []int  `json:"tags"`
}

// 落地到本地 JSONL 的一条记录，包一层元信息方便以后查询/去重。
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
		log.Fatalf("加载配置 %s 失败: %v", cfgPath, err)
	}

	// append-only 打开本地存储文件，单 writer 追加写。
	store, err := os.OpenFile(cfg.StoreFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("打开存储文件 %s 失败: %v", cfg.StoreFile, err)
	}
	defer store.Close()
	enc := json.NewEncoder(store) // 每次 Encode 自带换行，天然 JSONL

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		StartOffset: kafka.FirstOffset, // 仅当该 group 无已提交 offset 时生效（首次从最早开始，之后续读）
	})
	defer reader.Close()

	// Ctrl-C / SIGTERM 优雅退出
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("常驻消费 topic=%s group=%s，过滤 tag=%d，落地到 %s ...",
		cfg.Topic, cfg.GroupID, cfg.TargetTag, cfg.StoreFile)

	var hits int
	for {
		msg, err := reader.ReadMessage(ctx) // 自动提交 offset
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("收到退出信号，已停止。本次命中 %d 条 RWA_XSTOCK 事件", hits)
				return
			}
			log.Printf("读取消息出错: %v", err)
			continue
		}

		var ev tokenEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			log.Printf("跳过无法解析的消息 (offset=%d): %v", msg.Offset, err)
			continue
		}

		if !slices.Contains(ev.Tags, cfg.TargetTag) {
			continue // 非 RWA_XSTOCK，跳过
		}

		rec := storedEvent{
			ReceivedAt: time.Now(),
			Partition:  msg.Partition,
			Offset:     msg.Offset,
			KafkaTime:  msg.Time,
			Raw:        json.RawMessage(msg.Value),
		}
		if err := enc.Encode(&rec); err != nil {
			log.Printf("写入存储失败 (offset=%d): %v", msg.Offset, err)
			continue
		}

		hits++
		log.Printf("[RWA_XSTOCK] saved #%d  symbol=%s id=%s  (partition=%d offset=%d)",
			hits, ev.Symbol, ev.ID, msg.Partition, msg.Offset)
	}
}
