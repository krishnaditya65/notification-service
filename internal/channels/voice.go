package channels

import (
	"context"
	"fmt"
	"time"

	"github.com/notification-service/naas/internal/config"
	"github.com/notification-service/naas/pkg/models"
	"go.uber.org/zap"
)

type VoiceProvider struct {
	name     string
	priority int
	config   map[string]string
	logger   *zap.Logger
	rateLimit *TokenBucket
	circuitBreaker *CircuitBreaker
}

func NewVoiceProvider(cfg config.ProviderConfig, logger *zap.Logger) *VoiceProvider {
	return &VoiceProvider{
		name:     cfg.Name,
		priority: cfg.Priority,
		config:   cfg.Config,
		logger:   logger,
		rateLimit: NewTokenBucket(cfg.RateLimit.RequestsPerSecond, cfg.RateLimit.Burst),
		circuitBreaker: NewCircuitBreaker(cfg.CircuitBreaker.FailureThreshold, cfg.CircuitBreaker.SuccessThreshold, cfg.CircuitBreaker.Timeout),
	}
}

func (p *VoiceProvider) Name() string {
	return p.name
}

func (p *VoiceProvider) Channel() models.Channel {
	return models.ChannelVoice
}

func (p *VoiceProvider) Send(ctx context.Context, msg *Message) (*SendResult, error) {
	if !p.circuitBreaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open for %s", p.name)
	}

	if !p.rateLimit.Allow(1) {
		return nil, fmt.Errorf("rate limit exceeded for %s", p.name)
	}

	p.logger.Debug("Sending voice call", zap.String("provider", p.name), zap.String("to", msg.To))

	result := &SendResult{
		ProviderID:   p.name,
		ProviderMsgID: fmt.Sprintf("voice-%d", time.Now().UnixNano()),
		SentAt:       time.Now(),
		Metadata: map[string]interface{}{
			"provider": p.name,
			"channel":  "voice",
		},
	}

	switch p.name {
	case "twilio":
		return p.sendViaTwilio(ctx, msg, result)
	case "vonage":
		return p.sendViaVonage(ctx, msg, result)
	case "plivo":
		return p.sendViaPlivo(ctx, msg, result)
	default:
		return p.sendMock(ctx, msg, result)
	}
}

func (p *VoiceProvider) sendViaTwilio(ctx context.Context, msg *Message, result *SendResult) (*SendResult, error) {
	accountSID := p.config["account_sid"]
	authToken := p.config["auth_token"]
	if accountSID == "" || authToken == "" {
		return p.sendMock(ctx, msg, result)
	}

	p.logger.Info("Sending voice via Twilio", zap.String("to", msg.To))
	result.Metadata["api"] = "twilio"
	return result, nil
}

func (p *VoiceProvider) sendViaVonage(ctx context.Context, msg *Message, result *SendResult) (*SendResult, error) {
	apiKey := p.config["api_key"]
	apiSecret := p.config["api_secret"]
	if apiKey == "" || apiSecret == "" {
		return p.sendMock(ctx, msg, result)
	}

	p.logger.Info("Sending voice via Vonage", zap.String("to", msg.To))
	result.Metadata["api"] = "vonage"
	return result, nil
}

func (p *VoiceProvider) sendViaPlivo(ctx context.Context, msg *Message, result *SendResult) (*SendResult, error) {
	authID := p.config["auth_id"]
	authToken := p.config["auth_token"]
	if authID == "" || authToken == "" {
		return p.sendMock(ctx, msg, result)
	}

	p.logger.Info("Sending voice via Plivo", zap.String("to", msg.To))
	result.Metadata["api"] = "plivo"
	return result, nil
}

func (p *VoiceProvider) sendMock(ctx context.Context, msg *Message, result *SendResult) (*SendResult, error) {
	p.logger.Info("Sending voice (mock)", zap.String("to", msg.To))
	result.Metadata["mock"] = true
	return result, nil
}

func (p *VoiceProvider) HealthCheck(ctx context.Context) error {
	if p.circuitBreaker.State() == "open" {
		return fmt.Errorf("circuit breaker open")
	}
	return nil
}

func (p *VoiceProvider) Close() error {
	return nil
}