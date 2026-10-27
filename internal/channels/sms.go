package channels

import (
	"context"
	"fmt"
	"time"

	"github.com/notification-service/naas/internal/config"
	"github.com/notification-service/naas/pkg/models"
	"go.uber.org/zap"
)

type SMSProvider struct {
	name     string
	priority int
	config   map[string]string
	logger   *zap.Logger
	rateLimit *TokenBucket
	circuitBreaker *CircuitBreaker
}

func NewSMSProvider(cfg config.ProviderConfig, logger *zap.Logger) *SMSProvider {
	return &SMSProvider{
		name:     cfg.Name,
		priority: cfg.Priority,
		config:   cfg.Config,
		logger:   logger,
		rateLimit: NewTokenBucket(cfg.RateLimit.RequestsPerSecond, cfg.RateLimit.Burst),
		circuitBreaker: NewCircuitBreaker(cfg.CircuitBreaker.FailureThreshold, cfg.CircuitBreaker.SuccessThreshold, cfg.CircuitBreaker.Timeout),
	}
}

func (p *SMSProvider) Name() string {
	return p.name
}

func (p *SMSProvider) Channel() models.Channel {
	return models.ChannelSMS
}

func (p *SMSProvider) Send(ctx context.Context, msg *Message) (*SendResult, error) {
	if !p.circuitBreaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open for %s", p.name)
	}

	if !p.rateLimit.Allow(1) {
		return nil, fmt.Errorf("rate limit exceeded for %s", p.name)
	}

	p.logger.Debug("Sending SMS", zap.String("provider", p.name), zap.String("to", msg.To))

	result := &SendResult{
		ProviderID:   p.name,
		ProviderMsgID: fmt.Sprintf("sms-%d", time.Now().UnixNano()),
		SentAt:       time.Now(),
		Metadata: map[string]interface{}{
			"provider": p.name,
			"channel":  "sms",
		},
	}

	switch p.name {
	case "twilio":
		return p.sendViaTwilio(ctx, msg, result)
	case "plivo":
		return p.sendViaPlivo(ctx, msg, result)
	case "vonage":
		return p.sendViaVonage(ctx, msg, result)
	default:
		return p.sendMock(ctx, msg, result)
	}
}

func (p *SMSProvider) sendViaTwilio(ctx context.Context, msg *Message, result *SendResult) (*SendResult, error) {
	accountSID := p.config["account_sid"]
	authToken := p.config["auth_token"]
	if accountSID == "" || authToken == "" {
		return p.sendMock(ctx, msg, result)
	}

	p.logger.Info("Sending via Twilio", zap.String("to", msg.To))
	result.Metadata["api"] = "twilio"
	return result, nil
}

func (p *SMSProvider) sendViaPlivo(ctx context.Context, msg *Message, result *SendResult) (*SendResult, error) {
	authID := p.config["auth_id"]
	authToken := p.config["auth_token"]
	if authID == "" || authToken == "" {
		return p.sendMock(ctx, msg, result)
	}

	p.logger.Info("Sending via Plivo", zap.String("to", msg.To))
	result.Metadata["api"] = "plivo"
	return result, nil
}

func (p *SMSProvider) sendViaVonage(ctx context.Context, msg *Message, result *SendResult) (*SendResult, error) {
	apiKey := p.config["api_key"]
	apiSecret := p.config["api_secret"]
	if apiKey == "" || apiSecret == "" {
		return p.sendMock(ctx, msg, result)
	}

	p.logger.Info("Sending via Vonage", zap.String("to", msg.To))
	result.Metadata["api"] = "vonage"
	return result, nil
}

func (p *SMSProvider) sendMock(ctx context.Context, msg *Message, result *SendResult) (*SendResult, error) {
	p.logger.Info("Sending SMS (mock)", zap.String("to", msg.To), zap.String("body", msg.Body))
	result.Metadata["mock"] = true
	return result, nil
}

func (p *SMSProvider) HealthCheck(ctx context.Context) error {
	if p.circuitBreaker.State() == "open" {
		return fmt.Errorf("circuit breaker open")
	}
	return nil
}

func (p *SMSProvider) Close() error {
	return nil
}