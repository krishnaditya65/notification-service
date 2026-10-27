package models

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

var (
	ErrTemplateNotFound = errors.New("template not found")
	ErrNotificationNotFound = errors.New("notification not found")
	ErrUserPreferencesNotFound = errors.New("user preferences not found")
	ErrProviderNotFound = errors.New("provider not found")
)

type Store struct {
	DB    *pgxpool.Pool
	Redis *redis.Client
}

func NewStore(ctx context.Context, cfg DatabaseConfig) (*Store, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	poolConfig.MaxConns = int32(cfg.MaxOpenConns)
	poolConfig.MinConns = int32(cfg.MaxIdleConns)
	poolConfig.MaxConnLifetime = cfg.ConnMaxLifetime

	db, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(ctx); err != nil {
		return nil, err
	}

	return &Store{DB: db}, nil
}

func (s *Store) Close() {
	s.DB.Close()
}

func NewRedisClient(cfg RedisConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         cfg.Addresses[0],
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
	})
}

type Notification struct {
	ID            string                 `json:"id" db:"id"`
	TenantID      string                 `json:"tenant_id" db:"tenant_id"`
	UserID        string                 `json:"user_id" db:"user_id"`
	TemplateID    string                 `json:"template_id" db:"template_id"`
	Channel       Channel                `json:"channel" db:"channel"`
	Priority      Priority               `json:"priority" db:"priority"`
	Status        Status                 `json:"status" db:"status"`
	Payload       map[string]interface{} `json:"payload" db:"payload"`
	Variables     map[string]interface{} `json:"variables" db:"variables"`
	Locale        string                 `json:"locale" db:"locale"`
	ScheduledAt   *time.Time             `json:"scheduled_at,omitempty" db:"scheduled_at"`
	SentAt        *time.Time             `json:"sent_at,omitempty" db:"sent_at"`
	DeliveredAt   *time.Time             `json:"delivered_at,omitempty" db:"delivered_at"`
	OpenedAt      *time.Time             `json:"opened_at,omitempty" db:"opened_at"`
	ClickedAt     *time.Time             `json:"clicked_at,omitempty" db:"clicked_at"`
	Error         string                 `json:"error,omitempty" db:"error"`
	RetryCount    int                    `json:"retry_count" db:"retry_count"`
	MaxRetries    int                    `json:"max_retries" db:"max_retries"`
	Provider      string                 `json:"provider,omitempty" db:"provider"`
	ProviderID    string                 `json:"provider_id,omitempty" db:"provider_id"`
	IdempotencyKey string                `json:"idempotency_key,omitempty" db:"idempotency_key"`
	Metadata      map[string]interface{} `json:"metadata,omitempty" db:"metadata"`
	CreatedAt     time.Time              `json:"created_at" db:"created_at"`
	UpdatedAt     time.Time              `json:"updated_at" db:"updated_at"`
}

type Channel string

const (
	ChannelEmail   Channel = "email"
	ChannelSMS     Channel = "sms"
	ChannelPush    Channel = "push"
	ChannelInApp   Channel = "in_app"
	ChannelWebhook Channel = "webhook"
	ChannelVoice   Channel = "voice"
)

type Priority string

const (
	PriorityCritical Priority = "critical"
	PriorityHigh     Priority = "high"
	PriorityNormal   Priority = "normal"
	PriorityLow      Priority = "low"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusEnqueued   Status = "enqueued"
	StatusRouted     Status = "routed"
	StatusSent       Status = "sent"
	StatusDelivered  Status = "delivered"
	StatusOpened     Status = "opened"
	StatusClicked    Status = "clicked"
	StatusBounced    Status = "bounced"
	StatusFailed     Status = "failed"
	StatusSuppressed Status = "suppressed"
	StatusQueued     Status = "queued"
)

type Template struct {
	ID          string                 `json:"id" db:"id"`
	TenantID    string                 `json:"tenant_id" db:"tenant_id"`
	Name        string                 `json:"name" db:"name"`
	Channel     Channel                `json:"channel" db:"channel"`
	Subject     string                 `json:"subject" db:"subject"`
	Body        string                 `json:"body" db:"body"`
	Variables   []string               `json:"variables" db:"variables"`
	Locales     map[string]TemplateLocale `json:"locales" db:"locales"`
	Version     int                    `json:"version" db:"version"`
	IsActive    bool                   `json:"is_active" db:"is_active"`
	CreatedAt   time.Time              `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at" db:"updated_at"`
}

type TemplateLocale struct {
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type UserPreferences struct {
	UserID        string                 `json:"user_id" db:"user_id"`
	TenantID      string                 `json:"tenant_id" db:"tenant_id"`
	Channels      map[Channel]ChannelPref `json:"channels" db:"channels"`
	Categories    map[string]CategoryPref `json:"categories" db:"categories"`
	QuietHours    QuietHours             `json:"quiet_hours" db:"quiet_hours"`
	Timezone      string                 `json:"timezone" db:"timezone"`
	Locale        string                 `json:"locale" db:"locale"`
	FrequencyCaps map[string]FrequencyCap `json:"frequency_caps" db:"frequency_caps"`
	UpdatedAt     time.Time              `json:"updated_at" db:"updated_at"`
}

type ChannelPref struct {
	Enabled bool `json:"enabled"`
	OptedOut bool `json:"opted_out"`
}

type CategoryPref struct {
	Enabled   bool     `json:"enabled"`
	Channels  []Channel `json:"channels"`
	Frequency FrequencyCap `json:"frequency"`
}

type FrequencyCap struct {
	MaxCount    int           `json:"max_count"`
	Window      time.Duration `json:"window"`
	CurrentCount int          `json:"current_count"`
	WindowStart time.Time     `json:"window_start"`
}

type QuietHours struct {
	Enabled   bool   `json:"enabled"`
	StartHour int    `json:"start_hour"`
	EndHour   int    `json:"end_hour"`
	Timezone  string `json:"timezone"`
}

type RoutingRule struct {
	ID          string            `json:"id" db:"id"`
	TenantID    string            `json:"tenant_id" db:"tenant_id"`
	Name        string            `json:"name" db:"name"`
	Priority    int               `json:"priority" db:"priority"`
	Conditions  map[string]interface{} `json:"conditions" db:"conditions"`
	Actions     []RoutingAction   `json:"actions" db:"actions"`
	IsActive    bool              `json:"is_active" db:"is_active"`
	CreatedAt   time.Time         `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at" db:"updated_at"`
}

type RoutingAction struct {
	Type       string                 `json:"type"`
	Channel    Channel                `json:"channel,omitempty"`
	Provider   string                 `json:"provider,omitempty"`
	Delay      time.Duration          `json:"delay,omitempty"`
	Condition  string                 `json:"condition,omitempty"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
}

type Provider struct {
	ID           string            `json:"id" db:"id"`
	TenantID     string            `json:"tenant_id" db:"tenant_id"`
	Channel      Channel           `json:"channel" db:"channel"`
	Name         string            `json:"name" db:"name"`
	Config       map[string]string `json:"config" db:"config"`
	Priority     int               `json:"priority" db:"priority"`
	Enabled      bool              `json:"enabled" db:"enabled"`
	RateLimit    RateLimit         `json:"rate_limit" db:"rate_limit"`
	CircuitBreaker CircuitBreaker  `json:"circuit_breaker" db:"circuit_breaker"`
	CreatedAt    time.Time         `json:"created_at" db:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at" db:"updated_at"`
}

type RateLimit struct {
	RequestsPerSecond float64 `json:"requests_per_second"`
	Burst             int     `json:"burst"`
}

type CircuitBreaker struct {
	FailureThreshold int           `json:"failure_threshold"`
	SuccessThreshold int           `json:"success_threshold"`
	Timeout          time.Duration `json:"timeout"`
	State            string        `json:"state"`
	FailureCount     int           `json:"failure_count"`
	SuccessCount     int           `json:"success_count"`
	LastFailure      *time.Time    `json:"last_failure,omitempty"`
}

type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	TenantID  string                 `json:"tenant_id"`
	UserID    string                 `json:"user_id,omitempty"`
	Payload   map[string]interface{} `json:"payload"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

func (n *Notification) MarshalJSON() ([]byte, error) {
	type Alias Notification
	return json.Marshal(&struct {
		*Alias
		ScheduledAt string `json:"scheduled_at,omitempty"`
		SentAt      string `json:"sent_at,omitempty"`
		DeliveredAt string `json:"delivered_at,omitempty"`
		OpenedAt    string `json:"opened_at,omitempty"`
		ClickedAt   string `json:"clicked_at,omitempty"`
	}{
		Alias:       (*Alias)(n),
		ScheduledAt: formatTime(n.ScheduledAt),
		SentAt:      formatTime(n.SentAt),
		DeliveredAt: formatTime(n.DeliveredAt),
		OpenedAt:    formatTime(n.OpenedAt),
		ClickedAt:   formatTime(n.ClickedAt),
	})
}

func formatTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(time.RFC3339)
}

type DatabaseConfig struct {
	Host            string
	Port            int
	User            string
	Password        string
	Database        string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

type RedisConfig struct {
	Addresses     []string
	Password      string
	DB            int
	PoolSize      int
	MinIdleConns  int
}

type StoreInterface interface {
	GetTemplate(ctx context.Context, tenantID, templateID string) (*Template, error)
	GetTemplateByName(ctx context.Context, tenantID, name, channel string) (*Template, error)
	CreateTemplate(ctx context.Context, template *Template) error
	UpdateTemplate(ctx context.Context, template *Template) error
	DeleteTemplate(ctx context.Context, tenantID, templateID string) error
	ListTemplates(ctx context.Context, tenantID string, channel Channel) ([]*Template, error)
	GetNotification(ctx context.Context, id string) (*Notification, error)
	CreateNotification(ctx context.Context, notification *Notification) error
	UpdateNotification(ctx context.Context, notification *Notification) error
	ListNotifications(ctx context.Context, filter NotificationFilter) ([]*Notification, error)
	GetUserPreferences(ctx context.Context, tenantID, userID string) (*UserPreferences, error)
	UpsertUserPreferences(ctx context.Context, prefs *UserPreferences) error
	GetProvider(ctx context.Context, tenantID, channel string, providerName string) (*Provider, error)
	ListProviders(ctx context.Context, tenantID string, channel Channel) ([]*Provider, error)
	CreateProvider(ctx context.Context, provider *Provider) error
	UpdateProvider(ctx context.Context, provider *Provider) error
	GetRoutingRules(ctx context.Context, tenantID string) ([]*RoutingRule, error)
	CreateRoutingRule(ctx context.Context, rule *RoutingRule) error
	UpdateRoutingRule(ctx context.Context, rule *RoutingRule) error
}

type NotificationFilter struct {
	TenantID   string
	UserID     string
	Channel    Channel
	Status     Status
	Priority   Priority
	Limit      int
	Offset     int
	StartTime  *time.Time
	EndTime    *time.Time
}