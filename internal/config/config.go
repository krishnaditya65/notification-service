package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	Redis    RedisConfig
	Kafka    KafkaConfig
	Channels ChannelsConfig
	Routing  RoutingConfig
	Processing ProcessingConfig
	Reliability ReliabilityConfig
	Analytics AnalyticsConfig
}

type ServerConfig struct {
	Port         string
	Host         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
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
	Addresses  []string
	Password   string
	DB         int
	PoolSize   int
	MinIdleConns int
}

type KafkaConfig struct {
	Brokers  []string
	Topic    string
	GroupID  string
	Security string
}

type ChannelsConfig struct {
	Email    EmailConfig
	SMS      SMSConfig
	Push     PushConfig
	InApp    InAppConfig
	Webhook  WebhookConfig
	Voice    VoiceConfig
}

type EmailConfig struct {
	Providers []ProviderConfig
	DefaultProvider string
}

type SMSConfig struct {
	Providers []ProviderConfig
	DefaultProvider string
}

type PushConfig struct {
	APNs APNsConfig
	FCM  FCMConfig
}

type APNsConfig struct {
	KeyID      string
	TeamID     string
	BundleID   string
	PrivateKey string
	Production bool
}

type FCMConfig struct {
	ServerKey string
	ProjectID string
}

type InAppConfig struct {
	WebSocketPort int
	MaxConnections int
}

type WebhookConfig struct {
	Timeout time.Duration
	Retries int
}

type VoiceConfig struct {
	Providers []ProviderConfig
	DefaultProvider string
}

type ProviderConfig struct {
	Name        string
	Priority    int
	Enabled     bool
	Config      map[string]string
	RateLimit   RateLimitConfig
	CircuitBreaker CircuitBreakerConfig
}

type RateLimitConfig struct {
	RequestsPerSecond float64
	Burst             int
}

type CircuitBreakerConfig struct {
	FailureThreshold int
	SuccessThreshold int
	Timeout          time.Duration
}

type RoutingConfig struct {
	Tiers []TierConfig
	FallbackRules []FallbackRuleConfig
}

type TierConfig struct {
	Name        string
	Priority    int
	Channels    []string
	RateLimit   RateLimitConfig
}

type FallbackRuleConfig struct {
	FromChannel string
	ToChannel   string
	Delay       time.Duration
	Condition   string
}

type ProcessingConfig struct {
	DigestEnabled     bool
	DigestWindow      time.Duration
	MaxDigestSize     int
	BulkChunkSize     int
	IdempotencyTTL    time.Duration
}

type ReliabilityConfig struct {
	RetryPolicy RetryPolicyConfig
	DLQ         DLQConfig
	RateLimiter RateLimiterConfig
}

type RetryPolicyConfig struct {
	MaxRetries      int
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	Multiplier      float64
	RetryableErrors []string
}

type DLQConfig struct {
	MaxRetries int
	Retention  time.Duration
}

type RateLimiterConfig struct {
	GlobalRateLimit float64
	PerChannelLimit map[string]float64
}

type AnalyticsConfig struct {
	Enabled         bool
	EventTopic      string
	RetentionDays   int
	AuditEnabled    bool
	KafkaBrokers    []string
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