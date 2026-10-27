package processing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/notification-service/naas/pkg/models"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Processor struct {
	db           *pgxpool.Pool
	redis        *redis.Client
	logger       *zap.Logger
	config       *Config
	
	digestBuffers map[string]*DigestBuffer
	digestMu      sync.Mutex
	
	bulkJobs     map[string]*BulkJob
	bulkMu       sync.Mutex
	
	shutdownCh   chan struct{}
	wg           sync.WaitGroup
}

type Config struct {
	DigestEnabled  bool
	DigestWindow   time.Duration
	MaxDigestSize  int
	BulkChunkSize  int
	IdempotencyTTL time.Duration
}

type DigestBuffer struct {
	Key        string
	Events     []*models.Event
	Timer      *time.Timer
	FlushAt    time.Time
	mu         sync.Mutex
}

type BulkJob struct {
	ID           string
	TenantID     string
	TemplateID   string
	Channel      models.Channel
	UserIDs      []string
	Variables    map[string]interface{}
	Priority     models.Priority
	Status       string
	Processed    int
	Total        int
	StartedAt    time.Time
	CompletedAt  *time.Time
}

func (j *BulkJob) GetID() string { return j.ID }
func (j *BulkJob) GetTenantID() string { return j.TenantID }
func (j *BulkJob) GetTemplateID() string { return j.TemplateID }
func (j *BulkJob) GetChannel() models.Channel { return j.Channel }
func (j *BulkJob) GetUserIDs() []string { return j.UserIDs }
func (j *BulkJob) GetVariables() map[string]interface{} { return j.Variables }
func (j *BulkJob) GetPriority() models.Priority { return j.Priority }

func NewProcessor(db *pgxpool.Pool, redisClient *redis.Client, logger *zap.Logger) *Processor {
	return &Processor{
		db:            db,
		redis:         redisClient,
		logger:        logger,
		config:        &Config{},
		digestBuffers: make(map[string]*DigestBuffer),
		bulkJobs:      make(map[string]*BulkJob),
		shutdownCh:    make(chan struct{}),
	}
}

func (p *Processor) Configure(cfg *Config) {
	p.config = cfg
}

func (p *Processor) Start(ctx context.Context) error {
	p.wg.Add(1)
	go p.digestFlushWorker(ctx)
	return nil
}

func (p *Processor) Shutdown() {
	close(p.shutdownCh)
	p.wg.Wait()
}

func (p *Processor) CheckIdempotency(ctx context.Context, key string) (bool, error) {
	redisKey := fmt.Sprintf("idempotency:%s", key)
	exists, err := p.redis.Exists(ctx, redisKey).Result()
	if err != nil {
		return false, err
	}
	if exists > 0 {
		return true, nil
	}

	if err := p.redis.Set(ctx, redisKey, "1", p.config.IdempotencyTTL).Err(); err != nil {
		return false, err
	}
	return false, nil
}

func (p *Processor) GenerateIdempotencyKey(tenantID, userID, templateID, channel string, variables map[string]interface{}) string {
	data := fmt.Sprintf("%s:%s:%s:%s:%v", tenantID, userID, templateID, channel, variables)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

func (p *Processor) ProcessBulk(ctx context.Context, job *BulkJob) error {
	job.ID = fmt.Sprintf("bulk-%d", time.Now().UnixNano())
	job.Status = "processing"
	job.StartedAt = time.Now()
	job.Total = len(job.UserIDs)

	p.bulkMu.Lock()
	p.bulkJobs[job.ID] = job
	p.bulkMu.Unlock()

	p.wg.Add(1)
	go p.processBulkJob(ctx, job)
	return nil
}

func (p *Processor) ProcessBulkInterface(ctx context.Context, job interface {
	GetID() string
	GetTenantID() string
	GetTemplateID() string
	GetChannel() models.Channel
	GetUserIDs() []string
	GetVariables() map[string]interface{}
	GetPriority() models.Priority
}) error {
	bulkJob := &BulkJob{
		ID:          job.GetID(),
		TenantID:    job.GetTenantID(),
		TemplateID:  job.GetTemplateID(),
		Channel:     job.GetChannel(),
		UserIDs:     job.GetUserIDs(),
		Variables:   job.GetVariables(),
		Priority:    job.GetPriority(),
	}
	return p.ProcessBulk(ctx, bulkJob)
}

func (p *Processor) processBulkJob(ctx context.Context, job *BulkJob) {
	defer p.wg.Done()

	chunkSize := p.config.BulkChunkSize
	if chunkSize <= 0 {
		chunkSize = 1000
	}

	for i := 0; i < len(job.UserIDs); i += chunkSize {
		end := i + chunkSize
		if end > len(job.UserIDs) {
			end = len(job.UserIDs)
		}

		chunk := job.UserIDs[i:end]
		p.processChunk(ctx, job, chunk)

		select {
		case <-ctx.Done():
			job.Status = "cancelled"
			return
		case <-p.shutdownCh:
			job.Status = "cancelled"
			return
		default:
		}
	}

	now := time.Now()
	job.CompletedAt = &now
	job.Status = "completed"

	p.bulkMu.Lock()
	delete(p.bulkJobs, job.ID)
	p.bulkMu.Unlock()
}

func (p *Processor) processChunk(ctx context.Context, job *BulkJob, userIDs []string) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, 50)

	for _, userID := range userIDs {
		wg.Add(1)
		sem <- struct{}{}

		go func(uid string) {
			defer wg.Done()
			defer func() { <-sem }()

			job.Processed++
		}(userID)
	}

	wg.Wait()
}

func (p *Processor) AddToDigest(ctx context.Context, event *models.Event, digestKey string, window time.Duration) error {
	p.digestMu.Lock()
	defer p.digestMu.Unlock()

	buffer, ok := p.digestBuffers[digestKey]
	if !ok {
		buffer = &DigestBuffer{
			Key:     digestKey,
			Events:  make([]*models.Event, 0, p.config.MaxDigestSize),
			FlushAt: time.Now().Add(window),
		}
		p.digestBuffers[digestKey] = buffer

		buffer.Timer = time.AfterFunc(window, func() {
			p.flushDigest(ctx, digestKey)
		})
	}

	buffer.mu.Lock()
	buffer.Events = append(buffer.Events, event)
	shouldFlush := len(buffer.Events) >= p.config.MaxDigestSize
	buffer.mu.Unlock()

	if shouldFlush {
		p.flushDigest(ctx, digestKey)
	}

	return nil
}

func (p *Processor) flushDigest(ctx context.Context, digestKey string) {
	p.digestMu.Lock()
	buffer, ok := p.digestBuffers[digestKey]
	if !ok {
		p.digestMu.Unlock()
		return
	}
	delete(p.digestBuffers, digestKey)
	p.digestMu.Unlock()

	buffer.mu.Lock()
	events := buffer.Events
	buffer.mu.Unlock()

	if len(events) == 0 {
		return
	}

	if buffer.Timer != nil {
		buffer.Timer.Stop()
	}

	p.logger.Debug("Digest ready for processing", zap.String("digest_key", digestKey), zap.Int("event_count", len(events)))
}

func (p *Processor) digestFlushWorker(ctx context.Context) {
	defer p.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.shutdownCh:
			return
		case <-ticker.C:
			p.digestMu.Lock()
			now := time.Now()
			for key, buffer := range p.digestBuffers {
				if now.After(buffer.FlushAt) {
					delete(p.digestBuffers, key)
					go p.flushDigest(ctx, key)
				}
			}
			p.digestMu.Unlock()
		}
	}
}

func (p *Processor) GetBulkJobStatus(jobID string) (*BulkJob, bool) {
	p.bulkMu.Lock()
	defer p.bulkMu.Unlock()
	job, ok := p.bulkJobs[jobID]
	return job, ok
}

func (p *Processor) CreateDigestionRule(ctx context.Context, rule *DigestionRule) error {
	data, _ := json.Marshal(rule)
	return p.redis.Set(ctx, fmt.Sprintf("digest_rule:%s", rule.ID), data, 0).Err()
}

func (p *Processor) GetDigestionRule(ctx context.Context, ruleID string) (*DigestionRule, error) {
	data, err := p.redis.Get(ctx, fmt.Sprintf("digest_rule:%s", ruleID)).Bytes()
	if err != nil {
		return nil, err
	}
	var rule DigestionRule
	json.Unmarshal(data, &rule)
	return &rule, nil
}

type DigestionRule struct {
	ID          string        `json:"id"`
	TenantID    string        `json:"tenant_id"`
	Name        string        `json:"name"`
	EventTypes  []string      `json:"event_types"`
	GroupBy     []string      `json:"group_by"`
	Window      time.Duration `json:"window"`
	MaxSize     int           `json:"max_size"`
	TemplateID  string        `json:"template_id"`
	Channel     models.Channel `json:"channel"`
	Enabled     bool          `json:"enabled"`
}