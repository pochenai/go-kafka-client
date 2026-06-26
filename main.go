package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "os/signal"
    "slices"
    "strconv"
    "strings"
    "sync/atomic"
    "syscall"
    "time"

    "github.com/BurntSushi/toml"
    "github.com/segmentio/kafka-go"
)

type Config struct {
    Brokers       []string toml:"brokers"
    Topic         string   toml:"topic"
    GroupID       string   toml:"group_id"
    TargetTag     int      toml:"target_tag"
    StoreFile     string   toml:"store_file"
    HeartbeatSecs int      `toml:"heartbeat_secs"` // time-based liveness heartbeat interval in seconds; <=0 falls back to the default
}

// eventEnvelope is the outermost wrapper of the Kafka message; the real token
// structure lives in the data field.
type eventEnvelope struct {
    Topic  string     json:"topic"
    Source string     json:"source"
    Type   string     json:"type"
    Data   tokenEvent `json:"data"`
}

// multiplier is an RWA Scaled UI multiplier that may arrive on the wire as a JSON
// number (1) or a quoted string ("1.02604197021"); it normalizes both to float64.
// A null or empty value decodes to 0.
type multiplier float64

func (m *multiplier) UnmarshalJSON(data []byte) error {
    s := strings.Trim(string(data), `"`)
    if s == "" || s == "null" {
        *m = 0
        return nil
    }
    f, err := strconv.ParseFloat(s, 64)
    if err != nil {
        return err
    }
    *m = multiplier(f)
    return nil
}

// tokenLogicID is the chain index + token address that guarantees logical uniqueness.
type tokenLogicID struct {
    ChainIndex   int    json:"chainIndex"
    TokenAddress string json:"tokenAddress"
}

// tokenEvent 对应文档 gasless增量更新.md 中 data 字段的完整结构。
// tags 是 code 集合，RWA_XSTOCK = 36。可空字段用指针表达 null/缺省。
type tokenEvent struct {
    TokenLogicID           tokenLogicID `json:"tokenLogicId"`
    Name                   string       `json:"name"`                   // 脱敏后的名字
    Symbol                 string       `json:"symbol"`                 // 脱敏后的符号
    Decimal                int          `json:"decimal"`                // 精度，如 18
    CustomName             string       `json:"customName"`             // 业务线展示用自定义名字
    CustomSymbol           string       `json:"customSymbol"`           // 业务线展示用自定义符号
    LogoUrl                string       `json:"logoUrl"`                // Logo URL
    CoinTypeNo             int64        `json:"coinTypeNo"`             // 兼容钱包逻辑
    CreatedTime            int64        `json:"createdTime"`            // Token 创建时间(ms)
    UpdatedTime            int64        `json:"updatedTime"`            // Token 更新时间(ms)
    EventTime              int64        `json:"eventTime"`              // 事件生产时间(ms)，用于发现消费积压
    Protocol               string       `json:"protocol"`               // NFT 协议 / spl-token 等
    Tags                   []int        `json:"tags"`                   // tag code 集合
    Native                 bool         `json:"native"`                 // 本链原生代币
    TokenID                string       `json:"tokenId"`                // 链上唯一 id
    Rank                   int          `json:"rank"`                   // 排名
    Source                 string       `json:"source"`                 // 同步来源，如 "vault_add"
    IsDeleted              bool         `json:"isDeleted"`              // 是否已删除
    TokenCreatorAddress    string       `json:"tokenCreatorAddress"`    // 创建者地址
    TokenOnChainCreateTime int64        `json:"tokenOnChainCreateTime"` // 链上创建时间(ms)
    LogoUrlLarge           string       `json:"logoUrlLarge"`           // Logo 大图
    RawName                string       `json:"rawName"`                // 链上原始 name
    RawSymbol              string       `json:"rawSymbol"`              // 链上原始 symbol
    LogoPass               bool         `json:"logoPass"`               // logo 是否过审
    LatestMultiplier       multiplier   `json:"latestMultiplier"`       // RWA Scaled UI multiplier，线上可能是字符串或数字，统一成 float64
    MultiplierUpdatedAt    int64        `json:"multiplierUpdatedAt"`    // multiplier 更新时间(ms)
    IsRwa                  bool         `json:"isRwa"`                  // 是否 RWA
    ID                     string       `json:"id"`                     // chainIndex-tokenAddress
}

// storedEvent is one record persisted to the local JSONL file, wrapping the raw
// message with metadata for later querying/dedup.
type storedEvent struct {
    ReceivedAt time.Time       `json:"received_at"`
    Partition  int             json:"partition"
    Offset     int64           json:"offset"
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
        StartOffset: kafka.FirstOffset, // only applies when the group has no committed offset; with a fresh group this starts from the earliest retained message
        // Throughput: the default CommitInterval (0) makes ReadMessage commit the
        // offset synchronously on EVERY message — one broker round-trip per message,
        // which caps backlog scans at ~RTT^-1 msgs/sec. Batch commits every second
        // instead, and let each fetch pull a larger chunk.
        CommitInterval: time.Second,
        MinBytes:       10e3, // 10KB: wait for at least this much before returning a fetch
        MaxBytes:       10e6, // 10MB: cap per-fetch payload
    })
    defer reader.Close()

    // Graceful shutdown on Ctrl-C / SIGTERM
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    // Probe broker connectivity before consuming, then log the reader status.
    if err := probeBrokers(ctx, cfg.Brokers); err != nil {
        log.Fatalf("failed to connect to brokers %v: %v", cfg.Brokers, err)
    }
    // Report how far behind the head we are before starting to consume.
    lagCtx, lagCancel := context.WithTimeout(ctx, 15*time.Second)
    if lag, head, err := computeLag(lagCtx, cfg.Brokers, cfg.GroupID, cfg.Topic); err != nil {
        log.Printf("startup lag: skip, %v", err)
    } else {
        log.Printf("startup lag: behind head by %d messages (group=%s topic=%s head-offset=%d)",
            lag, cfg.GroupID, cfg.Topic, head)
    }
    lagCancel()

    log.Printf("consuming topic=%s group=%s, filtering tag=%d, storing to %s ...",
        cfg.Topic, cfg.GroupID, cfg.TargetTag, cfg.StoreFile)

    heartbeatSecs := cfg.HeartbeatSecs // time-based liveness heartbeat interval
    if heartbeatSecs <= 0 {
        heartbeatSecs = 10 // default when unset/invalid in config
    }

    // Counters shared with the heartbeat goroutine; atomic to avoid a data race.
    var read, hits, lastOffset, lastPartition, lastMsgMs atomic.Int64

    // Time-based heartbeat: unlike the count-based progress log, this fires on a wall
    // clock even when no messages are flowing — so a consumer that has caught up to the
    // head (or stalled) still reports liveness. It queries the live head each tick to
    // show the TRUE remaining lag: lag≈0 means caught up, lag not shrinking means stuck,
    // lag shrinking means still catching up.
    go func() {
        ticker := time.NewTicker(time.Duration(heartbeatSecs) * time.Second)
        defer ticker.Stop()
        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
                lag, head, err := computeLag(qctx, cfg.Brokers, cfg.GroupID, cfg.Topic)
                cancel()
                age := "n/a"
                if ms := lastMsgMs.Load(); ms > 0 {
                    age = time.Since(time.UnixMilli(ms)).Round(time.Second).String()
                }
                if err != nil {
                    log.Printf("heartbeat: alive read=%d hits=%d lastOffset=%d(p%d) lastMsgAge=%s lag=unknown(%v)",
                        read.Load(), hits.Load(), lastOffset.Load(), lastPartition.Load(), age, err)
                    continue
                }
                log.Printf("heartbeat: alive read=%d hits=%d lastOffset=%d(p%d) | behind head by %d msgs (head=%d) lastMsgAge=%s",
                    read.Load(), hits.Load(), lastOffset.Load(), lastPartition.Load(), lag, head, age)
            }
        }
    }()

    for {
        msg, err := reader.ReadMessage(ctx) // auto-commits offset
        if err != nil {
            if ctx.Err() != nil {
                log.Printf("received shutdown signal, stopped. matched %d RWA_XSTOCK events this run", hits.Load())
                return
            }
            log.Printf("error reading message: %v", err)
            continue
        }
        read.Add(1)
        lastOffset.Store(msg.Offset)
        lastPartition.Store(int64(msg.Partition))
        lastMsgMs.Store(msg.Time.UnixMilli())
        var env eventEnvelope
        if err := json.Unmarshal(msg.Value, &env); err != nil {
            log.Printf("skipping unparseable message (offset=%d): %v", msg.Offset, err)
            continue
        }
        ev := env.Data

        if !slices.Contains(ev.Tags, cfg.TargetTag) {
            continue // not RWA_XSTOCK, skip
        }

        fmt.Printf("Received event: %+v\n", ev) // debug print

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

        h := hits.Add(1)
        log.Printf("[RWA_XSTOCK] saved #%d  symbol=%s id=%s tags=%v isRwa=%v  (partition=%d offset=%d)",
            h, ev.Symbol, ev.ID, ev.Tags, ev.IsRwa, msg.Partition, msg.Offset)
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

// computeLag returns how far the consumer group is behind the head of the topic:
// lag = log-end-offset - committed-offset, summed across partitions, and head =
// summed log-end-offset. A partition the group has never committed is measured from
// its earliest available offset, matching the reader's FirstOffset start position.
// Callers treat it as best-effort: on error they log and carry on.
func computeLag(ctx context.Context, brokers []string, groupID, topic string) (totalLag, head int64, err error) {
    addr := kafka.TCP(brokers...)
    client := &kafka.Client{Addr: addr, Timeout: 10 * time.Second}

    meta, err := client.Metadata(ctx, &kafka.MetadataRequest{Addr: addr, Topics: []string{topic}})
    if err != nil {
        return 0, 0, fmt.Errorf("metadata request failed: %w", err)
    }
    if len(meta.Topics) == 0 || meta.Topics[0].Error != nil {
        return 0, 0, fmt.Errorf("topic %s unavailable: %v", topic, topicErr(meta))
    }

    // Ask for both ends of every partition in one request.
    reqs := make([]kafka.OffsetRequest, 0, len(meta.Topics[0].Partitions)*2)
    partitionIDs := make([]int, 0, len(meta.Topics[0].Partitions))
    for _, p := range meta.Topics[0].Partitions {
        reqs = append(reqs, kafka.FirstOffsetOf(p.ID), kafka.LastOffsetOf(p.ID))
        partitionIDs = append(partitionIDs, p.ID)
    }

    offsets, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
        Addr:   addr,
        Topics: map[string][]kafka.OffsetRequest{topic: reqs},
    })
    if err != nil {
        return 0, 0, fmt.Errorf("list offsets failed: %w", err)
    }

    commit, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
        Addr:    addr,
        GroupID: groupID,
        Topics:  map[string][]int{topic: partitionIDs},
    })
    if err != nil {
        return 0, 0, fmt.Errorf("offset fetch failed: %w", err)
    }
    committed := make(map[int]int64, len(partitionIDs)) // CommittedOffset is -1 if never committed
    for _, p := range commit.Topics[topic] {
        committed[p.Partition] = p.CommittedOffset
    }

    for _, po := range offsets.Topics[topic] {
        start := committed[po.Partition]
        if start < 0 {
            start = po.FirstOffset // group never committed -> reader starts from earliest
        }
        head += po.LastOffset
        totalLag += po.LastOffset - start
    }
    return totalLag, head, nil
}

// topicErr extracts the first topic-level error from a metadata response, if any.
func topicErr(meta *kafka.MetadataResponse) error {
    if len(meta.Topics) == 0 {
        return nil
    }
    return meta.Topics[0].Error
}