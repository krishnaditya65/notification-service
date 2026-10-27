package channels

import (
	"context"
	"fmt"
	"time"

	"github.com/notification-service/naas/internal/config"
	"github.com/notification-service/naas/pkg/models"
	"go.uber.org/zap"
)

type APNsProvider struct {
	config       config.APNsConfig
	logger       *zap.Logger
	rateLimit    *TokenBucket
	circuitBreaker *CircuitBreaker
}

func NewAPNsProvider(cfg config.APNsConfig, logger *zap.Logger) *APNsProvider {
	return &APNsProvider{
		config:        cfg,
		logger:        logger,
		rateLimit:     NewTokenBucket(1000, 2000),
		circuitBreaker: NewCircuitBreaker(5, 2, 30*time.Second),
	}
}

func (p *APNsProvider) Name() string {
	return "apns"
}

func (p *APNsProvider) Channel() models.Channel {
	return models.ChannelPush
}

func (p *APNsProvider) Send(ctx context.Context, msg *Message) (*SendResult, error) {
	if !p.circuitBreaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open for APNs")
	}

	if !p.rateLimit.Allow(1) {
		return nil, fmt.Errorf("rate limit exceeded for APNs")
	}

	p.logger.Debug("Sending push via APNs", zap.String("to", msg.To))

	result := &SendResult{
		ProviderID:   "apns",
		ProviderMsgID: fmt.Sprintf("apns-%d", time.Now().UnixNano()),
		SentAt:       time.Now(),
		Metadata: map[string]interface{}{
			"provider": "apns",
			"channel":  "push",
			"platform": "ios",
		},
	}

	if p.config.KeyID == "" || p.config.PrivateKey == "" {
		return p.sendMock(ctx, msg, result)
	}

	p.logger.Info("Sending via APNs", zap.String("device_token", msg.To))
	return result, nil
}

func (p *APNsProvider) sendMock(ctx context.Context, msg *Message, result *SendResult) (*SendResult, error) {
	p.logger.Info("Sending push via APNs (mock)", zap.String("device_token", msg.To))
	result.Metadata["mock"] = true
	return result, nil
}

func (p *APNsProvider) HealthCheck(ctx context.Context) error {
	if p.circuitBreaker.State() == "open" {
		return fmt.Errorf("circuit breaker open")
	}
	return nil
}

func (p *APNsProvider) Close() error {
	return nil
}

type FCMProvider struct {
	config        config.FCMConfig
	logger        *zap.Logger
	rateLimit     *TokenBucket
	circuitBreaker *CircuitBreaker
}

func NewFCMProvider(cfg config.FCMConfig, logger *zap.Logger) *FCMProvider {
	return &FCMProvider{
		config:         cfg,
		logger:         logger,
		rateLimit:      NewTokenBucket(1000, 2000),
		circuitBreaker: NewCircuitBreaker(5, 2, 30*time.Second),
	}
}

func (p *FCMProvider) Name() string {
	return "fcm"
}

func (p *FCMProvider) Channel() models.Channel {
	return models.ChannelPush
}

func (p *FCMProvider) Send(ctx context.Context, msg *Message) (*SendResult, error) {
	if !p.circuitBreaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open for FCM")
	}

	if !p.rateLimit.Allow(1) {
		return nil, fmt.Errorf("rate limit exceeded for FCM")
	}

	p.logger.Debug("Sending push via FCM", zap.String("to", msg.To))

	result := &SendResult{
		ProviderID:   "fcm",
		ProviderMsgID: fmt.Sprintf("fcm-%d", time.Now().UnixNano()),
		SentAt:       time.Now(),
		Metadata: map[string]interface{}{
			"provider": "fcm",
			"channel":  "push",
			"platform": "android",
		},
	}

	if p.config.ServerKey == "" {
		return p.sendMock(ctx, msg, result)
	}

	p.logger.Info("Sending via FCM", zap.String("device_token", msg.To))
	return result, nil
}

func (p *FCMProvider) sendMock(ctx context.Context, msg *Message, result *SendResult) (*SendResult, error) {
	p.logger.Info("Sending push via FCM (mock)", zap.String("device_token", msg.To))
	result.Metadata["mock"] = true
	return result, nil
}

func (p *FCMProvider) HealthCheck(ctx context.Context) error {
	if p.circuitBreaker.State() == "open" {
		return fmt.Errorf("circuit breaker open")
	}
	return nil
}

func (p *FCMProvider) Close() error {
	return nil
}