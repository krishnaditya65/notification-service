package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/notification-service/naas/internal/config"
	"github.com/notification-service/naas/pkg/models"
	"go.uber.org/zap"
)

type WebhookProvider struct {
	config        config.WebhookConfig
	logger        *zap.Logger
	httpClient    *http.Client
	rateLimit     *TokenBucket
	circuitBreaker *CircuitBreaker
}

func NewWebhookProvider(cfg config.WebhookConfig, logger *zap.Logger) *WebhookProvider {
	return &WebhookProvider{
		config:         cfg,
		logger:         logger,
		httpClient:     &http.Client{Timeout: cfg.Timeout},
		rateLimit:      NewTokenBucket(100, 200),
		circuitBreaker: NewCircuitBreaker(5, 2, 30*time.Second),
	}
}

func (p *WebhookProvider) Name() string {
	return "webhook"
}

func (p *WebhookProvider) Channel() models.Channel {
	return models.ChannelWebhook
}

func (p *WebhookProvider) Send(ctx context.Context, msg *Message) (*SendResult, error) {
	if !p.circuitBreaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open for webhook")
	}

	if !p.rateLimit.Allow(1) {
		return nil, fmt.Errorf("rate limit exceeded for webhook")
	}

	url := msg.Metadata["webhook_url"].(string)
	if url == "" {
		return nil, fmt.Errorf("webhook_url not provided in metadata")
	}

	payload := map[string]interface{}{
		"id":        msg.ID,
		"tenant_id": msg.TenantID,
		"user_id":   msg.UserID,
		"subject":   msg.Subject,
		"body":      msg.Body,
		"variables": msg.Variables,
		"timestamp": time.Now().Unix(),
	}

	body, _ := json.Marshal(payload)

	var lastErr error
	for attempt := 0; attempt <= p.config.Retries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * time.Second):
			}
		}

		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
		if err != nil {
			lastErr = err
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Notification-ID", msg.ID)
		req.Header.Set("X-Tenant-ID", msg.TenantID)

		resp, err := p.httpClient.Do(req)
		if err != nil {
			lastErr = err
			p.logger.Warn("Webhook request failed", zap.String("url", url), zap.Error(err))
			continue
		}
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return &SendResult{
				ProviderID:   "webhook",
				ProviderMsgID: fmt.Sprintf("webhook-%d", time.Now().UnixNano()),
				SentAt:       time.Now(),
				Metadata: map[string]interface{}{
					"provider":   "webhook",
					"channel":    "webhook",
					"status_code": resp.StatusCode,
				},
			}, nil
		}

		lastErr = fmt.Errorf("webhook returned status %d", resp.StatusCode)
		p.logger.Warn("Webhook returned error status", zap.String("url", url), zap.Int("status", resp.StatusCode))
	}

	return nil, fmt.Errorf("webhook failed after %d retries: %w", p.config.Retries, lastErr)
}

func (p *WebhookProvider) HealthCheck(ctx context.Context) error {
	if p.circuitBreaker.State() == "open" {
		return fmt.Errorf("circuit breaker open")
	}
	return nil
}

func (p *WebhookProvider) Close() error {
	return nil
}