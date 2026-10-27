package channels

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/notification-service/naas/internal/config"
	"github.com/notification-service/naas/pkg/models"
	"go.uber.org/zap"
)

type Provider interface {
	Name() string
	Channel() models.Channel
	Send(ctx context.Context, msg *Message) (*SendResult, error)
	HealthCheck(ctx context.Context) error
	Close() error
}

type Message struct {
	ID            string
	TenantID      string
	UserID        string
	To            string
	Subject       string
	Body          string
	Variables     map[string]interface{}
	Metadata      map[string]interface{}
	Priority      models.Priority
	Locale        string
}

type SendResult struct {
	ProviderID  string
	ProviderMsgID string
	SentAt      time.Time
	Metadata    map[string]interface{}
}

type Registry struct {
	providers map[models.Channel][]Provider
	mu        sync.RWMutex
	logger    *zap.Logger
	config    *config.Config
}

func NewRegistry(cfg *config.Config, logger *zap.Logger) *Registry {
	r := &Registry{
		providers: make(map[models.Channel][]Provider),
		logger:    logger,
		config:    cfg,
	}
	r.initProviders()
	return r
}

func (r *Registry) initProviders() {
	r.registerEmailProviders()
	r.registerSMSProviders()
	r.registerPushProviders()
	r.registerInAppProviders()
	r.registerWebhookProviders()
	r.registerVoiceProviders()
}

func (r *Registry) registerEmailProviders() {
	for _, pCfg := range r.config.Channels.Email.Providers {
		if !pCfg.Enabled {
			continue
		}
		provider := NewEmailProvider(pCfg, r.logger)
		r.providers[models.ChannelEmail] = append(r.providers[models.ChannelEmail], provider)
	}
}

func (r *Registry) registerSMSProviders() {
	for _, pCfg := range r.config.Channels.SMS.Providers {
		if !pCfg.Enabled {
			continue
		}
		provider := NewSMSProvider(pCfg, r.logger)
		r.providers[models.ChannelSMS] = append(r.providers[models.ChannelSMS], provider)
	}
}

func (r *Registry) registerPushProviders() {
	if r.config.Channels.Push.APNs.KeyID != "" {
		r.providers[models.ChannelPush] = append(r.providers[models.ChannelPush], NewAPNsProvider(r.config.Channels.Push.APNs, r.logger))
	}
	if r.config.Channels.Push.FCM.ServerKey != "" {
		r.providers[models.ChannelPush] = append(r.providers[models.ChannelPush], NewFCMProvider(r.config.Channels.Push.FCM, r.logger))
	}
}

func (r *Registry) registerInAppProviders() {
	r.providers[models.ChannelInApp] = append(r.providers[models.ChannelInApp], NewInAppProvider(r.config.Channels.InApp, r.logger))
}

func (r *Registry) registerWebhookProviders() {
	r.providers[models.ChannelWebhook] = append(r.providers[models.ChannelWebhook], NewWebhookProvider(r.config.Channels.Webhook, r.logger))
}

func (r *Registry) registerVoiceProviders() {
	for _, pCfg := range r.config.Channels.Voice.Providers {
		if !pCfg.Enabled {
			continue
		}
		provider := NewVoiceProvider(pCfg, r.logger)
		r.providers[models.ChannelVoice] = append(r.providers[models.ChannelVoice], provider)
	}
}

func (r *Registry) GetProviders(channel models.Channel) []Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.providers[channel]
}

func (r *Registry) GetProvider(channel models.Channel, name string) Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.providers[channel] {
		if p.Name() == name {
			return p
		}
	}
	return nil
}

func (r *Registry) GetDefaultProvider(channel models.Channel) Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	providers := r.providers[channel]
	if len(providers) == 0 {
		return nil
	}
	return providers[0]
}

func (r *Registry) HealthCheck(ctx context.Context) map[string]error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	results := make(map[string]error)
	for channel, providers := range r.providers {
		for _, p := range providers {
			key := fmt.Sprintf("%s:%s", channel, p.Name())
			results[key] = p.HealthCheck(ctx)
		}
	}
	return results
}

func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, providers := range r.providers {
		for _, p := range providers {
			if err := p.Close(); err != nil {
				r.logger.Error("Failed to close provider", zap.String("provider", p.Name()), zap.Error(err))
			}
		}
	}
	return nil
}