package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server      ServerConfig      `mapstructure:"server"`
	Database    DatabaseConfig    `mapstructure:"database"`
	Redis       RedisConfig       `mapstructure:"redis"`
	Kafka       KafkaConfig       `mapstructure:"kafka"`
	Channels    ChannelsConfig    `mapstructure:"channels"`
	Routing     RoutingConfig     `mapstructure:"routing"`
	Processing  ProcessingConfig  `mapstructure:"processing"`
	Reliability ReliabilityConfig `mapstructure:"reliability"`
	Analytics   AnalyticsConfig   `mapstructure:"analytics"`
}

type ServerConfig struct {
	Port         string        `mapstructure:"port"`
	Host         string        `mapstructure:"host"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	IdleTimeout  time.Duration `mapstructure:"idle_timeout"`
}

type DatabaseConfig struct {
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	User            string        `mapstructure:"user"`
	Password        string        `mapstructure:"password"`
	Database        string        `mapstructure:"database"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

type RedisConfig struct {
	Addresses    []string `mapstructure:"addresses"`
	Password     string   `mapstructure:"password"`
	DB           int      `mapstructure:"db"`
	PoolSize     int      `mapstructure:"pool_size"`
	MinIdleConns int      `mapstructure:"min_idle_conns"`
}

type KafkaConfig struct {
	Brokers  []string `mapstructure:"brokers"`
	Topic    string   `mapstructure:"topic"`
	GroupID  string   `mapstructure:"group_id"`
	Security string   `mapstructure:"security"`
}

type ChannelsConfig struct {
	Email   EmailConfig   `mapstructure:"email"`
	SMS     SMSConfig     `mapstructure:"sms"`
	Push    PushConfig    `mapstructure:"push"`
	InApp   InAppConfig   `mapstructure:"in_app"`
	Webhook WebhookConfig `mapstructure:"webhook"`
	Voice   VoiceConfig   `mapstructure:"voice"`
}

type EmailConfig struct {
	Providers       []ProviderConfig `mapstructure:"providers"`
	DefaultProvider string           `mapstructure:"default_provider"`
}

type SMSConfig struct {
	Providers       []ProviderConfig `mapstructure:"providers"`
	DefaultProvider string           `mapstructure:"default_provider"`
}

type PushConfig struct {
	APNs APNsConfig `mapstructure:"ap_ns"`
	FCM  FCMConfig  `mapstructure:"fcm"`
}

type APNsConfig struct {
	KeyID      string `mapstructure:"key_id"`
	TeamID     string `mapstructure:"team_id"`
	BundleID   string `mapstructure:"bundle_id"`
	PrivateKey string `mapstructure:"private_key"`
	Production bool   `mapstructure:"production"`
}

type FCMConfig struct {
	ServerKey string `mapstructure:"server_key"`
	ProjectID string `mapstructure:"project_id"`
}

type InAppConfig struct {
	WebSocketPort  int `mapstructure:"web_socket_port"`
	MaxConnections int `mapstructure:"max_connections"`
}

type WebhookConfig struct {
	Timeout time.Duration `mapstructure:"timeout"`
	Retries int           `mapstructure:"retries"`
}

type VoiceConfig struct {
	Providers       []ProviderConfig `mapstructure:"providers"`
	DefaultProvider string           `mapstructure:"default_provider"`
}

type ProviderConfig struct {
	Name           string               `mapstructure:"name"`
	Priority       int                  `mapstructure:"priority"`
	Enabled        bool                 `mapstructure:"enabled"`
	Config         map[string]string    `mapstructure:"config"`
	RateLimit      RateLimitConfig      `mapstructure:"rate_limit"`
	CircuitBreaker CircuitBreakerConfig `mapstructure:"circuit_breaker"`
}

type RateLimitConfig struct {
	RequestsPerSecond float64 `mapstructure:"requests_per_second"`
	Burst             int     `mapstructure:"burst"`
}

type CircuitBreakerConfig struct {
	FailureThreshold int           `mapstructure:"failure_threshold"`
	SuccessThreshold int           `mapstructure:"success_threshold"`
	Timeout          time.Duration `mapstructure:"timeout"`
}

type RoutingConfig struct {
	Tiers         []TierConfig         `mapstructure:"tiers"`
	FallbackRules []FallbackRuleConfig `mapstructure:"fallback_rules"`
}

type TierConfig struct {
	Name      string          `mapstructure:"name"`
	Priority  int             `mapstructure:"priority"`
	Channels  []string        `mapstructure:"channels"`
	RateLimit RateLimitConfig `mapstructure:"rate_limit"`
}

type FallbackRuleConfig struct {
	FromChannel string        `mapstructure:"from_channel"`
	ToChannel   string        `mapstructure:"to_channel"`
	Delay       time.Duration `mapstructure:"delay"`
	Condition   string        `mapstructure:"condition"`
}

type ProcessingConfig struct {
	DigestEnabled  bool          `mapstructure:"digest_enabled"`
	DigestWindow   time.Duration `mapstructure:"digest_window"`
	MaxDigestSize  int           `mapstructure:"max_digest_size"`
	BulkChunkSize  int           `mapstructure:"bulk_chunk_size"`
	IdempotencyTTL time.Duration `mapstructure:"idempotency_ttl"`
}

type ReliabilityConfig struct {
	RetryPolicy RetryPolicyConfig `mapstructure:"retry_policy"`
	DLQ         DLQConfig         `mapstructure:"dlq"`
	RateLimiter RateLimiterConfig `mapstructure:"rate_limiter"`
}

type RetryPolicyConfig struct {
	MaxRetries      int           `mapstructure:"max_retries"`
	InitialDelay    time.Duration `mapstructure:"initial_delay"`
	MaxDelay        time.Duration `mapstructure:"max_delay"`
	Multiplier      float64       `mapstructure:"multiplier"`
	RetryableErrors []string      `mapstructure:"retryable_errors"`
}

type DLQConfig struct {
	MaxRetries int           `mapstructure:"max_retries"`
	Retention  time.Duration `mapstructure:"retention"`
}

type RateLimiterConfig struct {
	GlobalRateLimit float64            `mapstructure:"global_rate_limit"`
	PerChannelLimit map[string]float64 `mapstructure:"per_channel_limit"`
}

type AnalyticsConfig struct {
	Enabled       bool     `mapstructure:"enabled"`
	EventTopic    string   `mapstructure:"event_topic"`
	RetentionDays int      `mapstructure:"retention_days"`
	AuditEnabled  bool     `mapstructure:"audit_enabled"`
	KafkaBrokers  []string `mapstructure:"kafka_brokers"`
}

func Load() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./configs")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	setDefaults()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, err
		}
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func setDefaults() {
	viper.SetDefault("server.port", "8080")
	viper.SetDefault("server.host", "0.0.0.0")
	viper.SetDefault("server.read_timeout", "30s")
	viper.SetDefault("server.write_timeout", "30s")
	viper.SetDefault("server.idle_timeout", "120s")

	viper.SetDefault("database.host", "localhost")
	viper.SetDefault("database.port", 5432)
	viper.SetDefault("database.max_open_conns", 25)
	viper.SetDefault("database.max_idle_conns", 5)
	viper.SetDefault("database.conn_max_lifetime", "5m")

	viper.SetDefault("redis.addresses", []string{"localhost:6379"})
	viper.SetDefault("redis.db", 0)
	viper.SetDefault("redis.pool_size", 100)
	viper.SetDefault("redis.min_idle_conns", 10)

	viper.SetDefault("kafka.brokers", []string{"localhost:9092"})
	viper.SetDefault("kafka.topic", "notifications")
	viper.SetDefault("kafka.group_id", "notification-service")

	viper.SetDefault("processing.digest_enabled", true)
	viper.SetDefault("processing.digest_window", "5m")
	viper.SetDefault("processing.max_digest_size", 50)
	viper.SetDefault("processing.bulk_chunk_size", 1000)
	viper.SetDefault("processing.idempotency_ttl", "24h")

	viper.SetDefault("reliability.retry_policy.max_retries", 3)
	viper.SetDefault("reliability.retry_policy.initial_delay", "1s")
	viper.SetDefault("reliability.retry_policy.max_delay", "30s")
	viper.SetDefault("reliability.retry_policy.multiplier", 2.0)
	viper.SetDefault("reliability.dlq.max_retries", 5)
	viper.SetDefault("reliability.dlq.retention", "7d")
	viper.SetDefault("reliability.rate_limiter.global_rate_limit", 10000)

	viper.SetDefault("analytics.enabled", true)
	viper.SetDefault("analytics.event_topic", "notification_events")
	viper.SetDefault("analytics.retention_days", 30)
	viper.SetDefault("analytics.audit_enabled", true)
	viper.SetDefault("analytics.kafka_brokers", []string{"localhost:9092"})
}
