package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Analytics struct {
	redis      *redis.Client
	kafka      *kafka.Writer
	logger     *zap.Logger
	config     *Config
	
	counters   map[string]*Counter
	countersMu sync.RWMutex
	
	tenantUsage map[string]*TenantUsage
	tenantMu    sync.RWMutex
}

type Config struct {
	Enabled       bool
	EventTopic    string
	RetentionDays int
	AuditEnabled  bool
	KafkaBrokers  []string
}

type Counter struct {
	name      string
	labels    map[string]string
	value     int64
	lastReset time.Time
}

type TenantUsage struct {
	TenantID       string
	MessagesSent   int64
	MessagesFailed int64
	ByChannel      map[string]int64
	ByProvider     map[string]int64
	LastUpdated    time.Time
}

type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	TenantID  string                 `json:"tenant_id"`
	UserID    string                 `json:"user_id,omitempty"`
	Channel   string                 `json:"channel,omitempty"`
	Provider  string                 `json:"provider,omitempty"`
	Status    string                 `json:"status,omitempty"`
	LatencyMs int64                  `json:"latency_ms,omitempty"`
	Cost      float64                `json:"cost,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

func NewAnalytics(redisClient *redis.Client, cfg *Config, logger *zap.Logger) *Analytics {
	a := &Analytics{
		redis:       redisClient,
		logger:      logger,
		config:      cfg,
		counters:    make(map[string]*Counter),
		tenantUsage: make(map[string]*TenantUsage),
	}

	if cfg.Enabled && len(cfg.KafkaBrokers) > 0 {
		a.kafka = &kafka.Writer{
			Addr:         kafka.TCP(cfg.KafkaBrokers...),
			Topic:        cfg.EventTopic,
			Balancer:     &kafka.LeastBytes{},
			BatchSize:    100,
			BatchTimeout: 10 * time.Millisecond,
		}
	}

	return a
}

func (a *Analytics) TrackEvent(ctx context.Context, event *Event) error {
	if !a.config.Enabled {
		return nil
	}

	event.Timestamp = time.Now()

	a.incrementCounter("events_total", map[string]string{
		"type":   event.Type,
		"tenant": event.TenantID,
	})

	if event.Channel != "" {
		a.incrementCounter("events_by_channel", map[string]string{
			"channel": event.Channel,
			"tenant":  event.TenantID,
		})
	}

	if event.Status != "" {
		a.incrementCounter("events_by_status", map[string]string{
			"status": event.Status,
			"tenant": event.TenantID,
		})
	}

	a.updateTenantUsage(event)

	if a.kafka != nil {
		data, _ := json.Marshal(event)
		return a.kafka.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.TenantID),
			Value: data,
		})
	}

	return a.storeEventRedis(ctx, event)
}

func (a *Analytics) incrementCounter(name string, labels map[string]string) {
	key := name
	for k, v := range labels {
		key += fmt.Sprintf(":%s=%s", k, v)
	}

	a.countersMu.Lock()
	defer a.countersMu.Unlock()

	if counter, ok := a.counters[key]; ok {
		counter.value++
	} else {
		a.counters[key] = &Counter{
			name:      name,
			labels:    labels,
			value:     1,
			lastReset: time.Now(),
		}
	}
}

func (a *Analytics) updateTenantUsage(event *Event) {
	a.tenantMu.Lock()
	defer a.tenantMu.Unlock()

	usage, ok := a.tenantUsage[event.TenantID]
	if !ok {
		usage = &TenantUsage{
			TenantID:  event.TenantID,
			ByChannel: make(map[string]int64),
			ByProvider: make(map[string]int64),
		}
		a.tenantUsage[event.TenantID] = usage
	}

	usage.LastUpdated = time.Now()

	switch event.Type {
	case "sent", "delivered":
		usage.MessagesSent++
	case "failed", "bounced":
		usage.MessagesFailed++
	}

	if event.Channel != "" {
		usage.ByChannel[event.Channel]++
	}
	if event.Provider != "" {
		usage.ByProvider[event.Provider]++
	}
}

func (a *Analytics) storeEventRedis(ctx context.Context, event *Event) error {
	data, _ := json.Marshal(event)
	key := fmt.Sprintf("events:%s:%s", event.TenantID, event.Timestamp.Format("2006-01-02"))
	return a.redis.LPush(ctx, key, data).Err()
}

func (a *Analytics) GetTenantUsage(ctx context.Context, tenantID string) (*TenantUsage, error) {
	a.tenantMu.RLock()
	usage, ok := a.tenantUsage[tenantID]
	a.tenantMu.RUnlock()

	if ok {
		return usage, nil
	}

	key := fmt.Sprintf("usage:%s", tenantID)
	data, err := a.redis.Get(ctx, key).Bytes()
	if err != nil {
		return &TenantUsage{TenantID: tenantID}, nil
	}

	json.Unmarshal(data, &usage)
	return usage, nil
}

func (a *Analytics) GetCounters() map[string]int64 {
	a.countersMu.RLock()
	defer a.countersMu.RUnlock()

	result := make(map[string]int64)
	for key, counter := range a.counters {
		result[key] = counter.value
	}
	return result
}

func (a *Analytics) GetEvents(ctx context.Context, tenantID, date string, limit int) ([]*Event, error) {
	key := fmt.Sprintf("events:%s:%s", tenantID, date)
	data, err := a.redis.LRange(ctx, key, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}

	var events []*Event
	for _, d := range data {
		var event Event
		if json.Unmarshal([]byte(d), &event) == nil {
			events = append(events, &event)
		}
	}
	return events, nil
}

func (a *Analytics) FlushCounters(ctx context.Context) error {
	a.countersMu.Lock()
	defer a.countersMu.Unlock()

	for key, counter := range a.counters {
		counterKey := fmt.Sprintf("counter:%s:%s", counter.name, counter.lastReset.Format("2006-01-02"))
		data, _ := json.Marshal(map[string]interface{}{
			"name":   counter.name,
			"labels": counter.labels,
			"value":  counter.value,
			"date":   counter.lastReset.Format("2006-01-02"),
		})
		a.redis.Set(ctx, counterKey+":"+key, data, time.Duration(a.config.RetentionDays)*24*time.Hour).Err()
		counter.value = 0
		counter.lastReset = time.Now()
	}

	a.tenantMu.Lock()
	for tenantID, usage := range a.tenantUsage {
		key := fmt.Sprintf("usage:%s", tenantID)
		data, _ := json.Marshal(usage)
		a.redis.Set(ctx, key, data, time.Duration(a.config.RetentionDays)*24*time.Hour).Err()
	}
	a.tenantMu.Unlock()

	return nil
}

func (a *Analytics) StartFlushWorker(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.FlushCounters(ctx)
			}
		}
	}()
}

func (a *Analytics) AuditLog(ctx context.Context, eventType, tenantID, userID, resourceID, action string, details map[string]interface{}) error {
	if !a.config.AuditEnabled {
		return nil
	}

	audit := map[string]interface{}{
		"event_type":  eventType,
		"tenant_id":   tenantID,
		"user_id":     userID,
		"resource_id": resourceID,
		"action":      action,
		"details":     details,
		"timestamp":   time.Now().Unix(),
	}

	data, _ := json.Marshal(audit)
	key := fmt.Sprintf("audit:%s:%s", tenantID, time.Now().Format("2006-01-02"))
	return a.redis.LPush(ctx, key, data).Err()
}

func (a *Analytics) GetAuditLogs(ctx context.Context, tenantID, date string, limit int) ([]map[string]interface{}, error) {
	key := fmt.Sprintf("audit:%s:%s", tenantID, date)
	data, err := a.redis.LRange(ctx, key, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}

	var logs []map[string]interface{}
	for _, d := range data {
		var log map[string]interface{}
		if json.Unmarshal([]byte(d), &log) == nil {
			logs = append(logs, log)
		}
	}
	return logs, nil
}

func (a *Analytics) Close() error {
	if a.kafka != nil {
		return a.kafka.Close()
	}
	return nil
}