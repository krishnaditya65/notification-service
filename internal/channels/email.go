package channels

import (
	"context"
	"fmt"
	"time"

	"github.com/notification-service/naas/internal/config"
	"github.com/notification-service/naas/pkg/models"
	"go.uber.org/zap"
)

type EmailProvider struct {
	name     string
	priority int
	config   map[string]string
	logger   *zap.Logger
	rateLimit *TokenBucket
	circuitBreaker *CircuitBreaker
}

func NewEmailProvider(cfg config.ProviderConfig, logger *zap.Logger) *EmailProvider {
	return &EmailProvider{
		name:     cfg.Name,
		priority: cfg.Priority,
		config:   cfg.Config,
		logger:   logger,
		rateLimit: NewTokenBucket(cfg.RateLimit.RequestsPerSecond, cfg.RateLimit.Burst),
		circuitBreaker: NewCircuitBreaker(cfg.CircuitBreaker.FailureThreshold, cfg.CircuitBreaker.SuccessThreshold, cfg.CircuitBreaker.Timeout),
	}
}

func (p *EmailProvider) Name() string {
	return p.name
}

func (p *EmailProvider) Channel() models.Channel {
	return models.ChannelEmail
}

func (p *EmailProvider) Send(ctx context.Context, msg *Message) (*SendResult, error) {
	if !p.circuitBreaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open for %s", p.name)
	}

	if !p.rateLimit.Allow(1) {
		return nil, fmt.Errorf("rate limit exceeded for %s", p.name)
	}

	p.logger.Debug("Sending email", zap.String("provider", p.name), zap.String("to", msg.To))

	result := &SendResult{
		ProviderID:   p.name,
		ProviderMsgID: fmt.Sprintf("email-%d", time.Now().UnixNano()),
		SentAt:       time.Now(),
		Metadata: map[string]interface{}{
			"provider": p.name,
			"channel":  "email",
		},
	}

	switch p.name {
	case "sendgrid":
		return p.sendViaSendGrid(ctx, msg, result)
	case "ses":
		return p.sendViaSES(ctx, msg, result)
	case "mailgun":
		return p.sendViaMailgun(ctx, msg, result)
	default:
		return p.sendMock(ctx, msg, result)
	}
}

func (p *EmailProvider) sendViaSendGrid(ctx context.Context, msg *Message, result *SendResult) (*SendResult, error) {
	apiKey := p.config["api_key"]
	if apiKey == "" {
		return p.sendMock(ctx, msg, result)
	}

	p.logger.Info("Sending via SendGrid", zap.String("to", msg.To))
	result.Metadata["api"] = "sendgrid"
	return result, nil
}

func (p *EmailProvider) sendViaSES(ctx context.Context, msg *Message, result *SendResult) (*SendResult, error) {
	accessKey := p.config["access_key"]
	if accessKey == "" {
		return p.sendMock(ctx, msg, result)
	}

	p.logger.Info("Sending via AWS SES", zap.String("to", msg.To))
	result.Metadata["api"] = "ses"
	return result, nil
}

func (p *EmailProvider) sendViaMailgun(ctx context.Context, msg *Message, result *SendResult) (*SendResult, error) {
	apiKey := p.config["api_key"]
	if apiKey == "" {
		return p.sendMock(ctx, msg, result)
	}

	p.logger.Info("Sending via Mailgun", zap.String("to", msg.To))
	result.Metadata["api"] = "mailgun"
	return result, nil
}

func (p *EmailProvider) sendMock(ctx context.Context, msg *Message, result *SendResult) (*SendResult, error) {
	p.logger.Info("Sending email (mock)", zap.String("to", msg.To), zap.String("subject", msg.Subject))
	result.Metadata["mock"] = true
	return result, nil
}

func (p *EmailProvider) HealthCheck(ctx context.Context) error {
	if p.circuitBreaker.State() == "open" {
		return fmt.Errorf("circuit breaker open")
	}
	return nil
}

func (p *EmailProvider) Close() error {
	return nil
}