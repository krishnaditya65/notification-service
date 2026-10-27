package reliability

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/notification-service/naas/internal/config"
	"github.com/notification-service/naas/pkg/models"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type ProcessorInterface interface {
	ProcessBulk(ctx context.Context, job interface{}) error
	AddToDigest(ctx context.Context, event interface{}, digestKey string, window time.Duration) error
	GenerateIdempotencyKey(tenantID, userID, templateID, channel string, variables map[string]interface{}) string
}

type Executor interface {
	Execute(ctx context.Context, notification *models.Notification, processor interface{}) error
}

type Manager struct {
	db           *pgxpool.Pool
	redis        *redis.Client
	logger       *zap.Logger
	config       *config.ReliabilityConfig
	
	retryQueues   map[models.Priority]chan *RetryTask
	retryWorkers  map[models.Priority][]*RetryWorker
	dlqWorker     *DLQWorker
	globalLimiter *GlobalRateLimiter
	channelLimiters map[string]*TokenBucket
	
	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

type RetryTask struct {
	Notification *models.Notification
	Attempt      int
	LastError    string
	NextRetryAt  time.Time
}

type RetryWorker struct {
	priority    models.Priority
	taskQueue   chan *RetryTask
	orchestrator Executor
	logger      *zap.Logger
	config      *config.RetryPolicyConfig
	shutdownCh  chan struct{}
}

type DLQWorker struct {
	db      *pgxpool.Pool
	redis   *redis.Client
	logger  *zap.Logger
	config  *config.DLQConfig
	queue   chan *DLQEntry
	shutdownCh chan struct{}
}

type DLQEntry struct {
	Notification *models.Notification
	Error        string
	Attempts     int
	FailedAt     time.Time
	Provider     string
}

type GlobalRateLimiter struct {
	bucket    *TokenBucket
	channelBuckets map[string]*TokenBucket
	mu        sync.RWMutex
}

func NewManager(db *pgxpool.Pool, redisClient *redis.Client, cfg *config.Config, logger *zap.Logger) *Manager {
	m := &Manager{
		db:         db,
		redis:      redisClient,
		logger:     logger,
		config:     &cfg.Reliability,
		retryQueues: make(map[models.Priority]chan *RetryTask),
		retryWorkers: make(map[models.Priority][]*RetryWorker),
		channelLimiters: make(map[string]*TokenBucket),
		shutdownCh: make(chan struct{}),
	}

	m.initRetryQueues()
	m.initRateLimiters()
	m.initDLQ()

	return m
}

func (m *Manager) initRetryQueues() {
	priorities := []models.Priority{
		models.PriorityCritical,
		models.PriorityHigh,
		models.PriorityNormal,
		models.PriorityLow,
	}

	for _, p := range priorities {
		queueSize := 1000
		if p == models.PriorityCritical {
			queueSize = 5000
		}
		m.retryQueues[p] = make(chan *RetryTask, queueSize)
	}
}

func (m *Manager) initRateLimiters() {
	globalRate := m.config.RateLimiter.GlobalRateLimit
	if globalRate <= 0 {
		globalRate = 10000
	}
	m.globalLimiter = &GlobalRateLimiter{
		bucket: NewTokenBucket(globalRate, int(globalRate*2)),
		channelBuckets: make(map[string]*TokenBucket),
	}

	for channel, limit := range m.config.RateLimiter.PerChannelLimit {
		if limit > 0 {
			m.channelLimiters[channel] = NewTokenBucket(limit, int(limit*2))
		}
	}
}

func (m *Manager) initDLQ() {
	m.dlqWorker = &DLQWorker{
		db:         m.db,
		redis:      m.redis,
		logger:     m.logger,
		config:     &m.config.DLQ,
		queue:      make(chan *DLQEntry, 10000),
		shutdownCh: make(chan struct{}),
	}
}

func (m *Manager) Start(ctx context.Context, orchestrator Executor) error {
	m.wg.Add(1)
	go m.dlqWorker.run(m.shutdownCh)

	for priority, queue := range m.retryQueues {
		workerCount := 5
		if priority == models.PriorityCritical {
			workerCount = 20
		}

		for i := 0; i < workerCount; i++ {
			worker := &RetryWorker{
				priority:     priority,
				taskQueue:    queue,
				orchestrator: orchestrator,
				logger:       m.logger,
				config:       &m.config.RetryPolicy,
				shutdownCh:   make(chan struct{}),
			}
			m.retryWorkers[priority] = append(m.retryWorkers[priority], worker)
			m.wg.Add(1)
			go worker.run(m.shutdownCh)
		}
	}

	return nil
}

func (m *Manager) Shutdown() {
	close(m.shutdownCh)
	m.wg.Wait()
}

func (m *Manager) ScheduleRetry(ctx context.Context, notification *models.Notification, err error) error {
	attempt := notification.RetryCount + 1
	maxRetries := notification.MaxRetries
	if maxRetries <= 0 {
		maxRetries = m.config.RetryPolicy.MaxRetries
	}

	if attempt > maxRetries {
		return m.sendToDLQ(ctx, notification, fmt.Sprintf("max retries exceeded: %v", err))
	}

	delay := m.calculateBackoff(attempt)
	nextRetryAt := time.Now().Add(delay)

	task := &RetryTask{
		Notification: notification,
		Attempt:      attempt,
		LastError:    err.Error(),
		NextRetryAt:  nextRetryAt,
	}

	select {
	case m.retryQueues[notification.Priority] <- task:
		m.logger.Debug("Scheduled retry", zap.String("notification_id", notification.ID), zap.Int("attempt", attempt), zap.Duration("delay", delay))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return m.sendToDLQ(ctx, notification, "retry queue full")
	}
}

func (m *Manager) calculateBackoff(attempt int) time.Duration {
	policy := m.config.RetryPolicy
	delay := policy.InitialDelay * time.Duration(math.Pow(policy.Multiplier, float64(attempt-1)))
	if delay > policy.MaxDelay {
		delay = policy.MaxDelay
	}
	return delay
}

func (m *Manager) sendToDLQ(ctx context.Context, notification *models.Notification, errMsg string) error {
	entry := &DLQEntry{
		Notification: notification,
		Error:        errMsg,
		Attempts:     notification.RetryCount,
		FailedAt:     time.Now(),
		Provider:     notification.Provider,
	}

	select {
	case m.dlqWorker.queue <- entry:
		m.logger.Warn("Sent to DLQ", zap.String("notification_id", notification.ID), zap.String("error", errMsg))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return m.dlqWorker.persistDLQ(ctx, entry)
	}
}

func (m *Manager) AllowSend(channel models.Channel) bool {
	if !m.globalLimiter.bucket.Allow(1) {
		return false
	}

	m.globalLimiter.mu.RLock()
	bucket, ok := m.globalLimiter.channelBuckets[string(channel)]
	m.globalLimiter.mu.RUnlock()

	if ok && !bucket.Allow(1) {
		return false
	}

	if channelLimiter, ok := m.channelLimiters[string(channel)]; ok {
		return channelLimiter.Allow(1)
	}

	return true
}

func (m *Manager) WaitForCapacity(channel models.Channel) time.Duration {
	wait := m.globalLimiter.bucket.Wait(1)

	m.globalLimiter.mu.RLock()
	bucket, ok := m.globalLimiter.channelBuckets[string(channel)]
	m.globalLimiter.mu.RUnlock()

	if ok {
		channelWait := bucket.Wait(1)
		if channelWait > wait {
			wait = channelWait
		}
	}

	if channelLimiter, ok := m.channelLimiters[string(channel)]; ok {
		channelWait := channelLimiter.Wait(1)
		if channelWait > wait {
			wait = channelWait
		}
	}

	return wait
}

func (rw *RetryWorker) run(shutdownCh chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-shutdownCh:
			return
		case task := <-rw.taskQueue:
			if time.Now().Before(task.NextRetryAt) {
				select {
				case <-time.After(time.Until(task.NextRetryAt)):
				case <-shutdownCh:
					return
				}
			}

			task.Notification.RetryCount = task.Attempt
			task.Notification.Error = task.LastError

			if err := rw.orchestrator.Execute(context.Background(), task.Notification, nil); err != nil {
				rw.logger.Error("Retry failed", zap.String("notification_id", task.Notification.ID), zap.Int("attempt", task.Attempt), zap.Error(err))
			}
		case <-ticker.C:
		}
	}
}

func (dw *DLQWorker) run(shutdownCh chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	batch := make([]*DLQEntry, 0, 100)

	for {
		select {
		case <-shutdownCh:
			if len(batch) > 0 {
				dw.flushBatch(context.Background(), batch)
			}
			return
		case entry := <-dw.queue:
			batch = append(batch, entry)
			if len(batch) >= 100 {
				dw.flushBatch(context.Background(), batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				dw.flushBatch(context.Background(), batch)
				batch = batch[:0]
			}
		}
	}
}

func (dw *DLQWorker) flushBatch(ctx context.Context, entries []*DLQEntry) {
	for _, entry := range entries {
		dw.persistDLQ(ctx, entry)
	}
}

func (dw *DLQWorker) persistDLQ(ctx context.Context, entry *DLQEntry) error {
	notification := entry.Notification
	notification.Status = models.StatusFailed
	notification.Error = entry.Error
	notification.RetryCount = entry.Attempts

	data, _ := json.Marshal(notification)

	query := `
		INSERT INTO dead_letter_queue (id, tenant_id, user_id, notification_data, error, attempts, failed_at, provider)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET
			attempts = EXCLUDED.attempts,
			error = EXCLUDED.error,
			failed_at = EXCLUDED.failed_at
	`

	_, err := dw.db.Exec(ctx, query,
		notification.ID, notification.TenantID, notification.UserID,
		data, entry.Error, entry.Attempts, entry.FailedAt, entry.Provider,
	)

	if err != nil {
		dw.logger.Error("Failed to persist DLQ entry", zap.String("notification_id", notification.ID), zap.Error(err))
	}

	dw.redis.LPush(ctx, "dlq:recent", string(data)).Err()
	dw.redis.LTrim(ctx, "dlq:recent", 0, 9999).Err()

	return err
}

func (m *Manager) GetDLQEntries(ctx context.Context, limit, offset int) ([]*DLQEntry, error) {
	query := `
		SELECT id, tenant_id, user_id, notification_data, error, attempts, failed_at, provider
		FROM dead_letter_queue
		ORDER BY failed_at DESC
		LIMIT $1 OFFSET $2
	`

	rows, err := m.db.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*DLQEntry
	for rows.Next() {
		var entry DLQEntry
		var notificationData []byte

		err := rows.Scan(&entry.Notification.ID, &entry.Notification.TenantID, &entry.Notification.UserID, &notificationData, &entry.Error, &entry.Attempts, &entry.FailedAt, &entry.Provider)
		if err != nil {
			return nil, err
		}

		json.Unmarshal(notificationData, &entry.Notification)
		entries = append(entries, &entry)
	}

	return entries, rows.Err()
}

func (m *Manager) RetryDLQEntry(ctx context.Context, notificationID string, orchestrator Executor) error {
	query := `SELECT notification_data FROM dead_letter_queue WHERE id = $1`
	var data []byte
	err := m.db.QueryRow(ctx, query, notificationID).Scan(&data)
	if err != nil {
		return err
	}

	var notification models.Notification
	json.Unmarshal(data, &notification)
	notification.RetryCount = 0
	notification.Error = ""
	notification.Status = models.StatusPending

	return orchestrator.Execute(ctx, &notification, nil)
}

func (m *Manager) UpdateChannelRateLimit(channel string, ratePerSec float64, burst int) {
	m.globalLimiter.mu.Lock()
	defer m.globalLimiter.mu.Unlock()
	m.globalLimiter.channelBuckets[channel] = NewTokenBucket(ratePerSec, burst)
}

func (m *Manager) GetRateLimitStatus() map[string]interface{} {
	status := map[string]interface{}{
		"global": map[string]interface{}{
			"rate": m.config.RateLimiter.GlobalRateLimit,
		},
		"channels": make(map[string]interface{}),
	}

	for channel, bucket := range m.channelLimiters {
		status["channels"].(map[string]interface{})[channel] = map[string]interface{}{
			"rate":   bucket.ratePerSec,
			"burst":  bucket.capacity,
		}
	}

	return status
}