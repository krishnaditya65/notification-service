package routing

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/notification-service/naas/internal/channels"
	"github.com/notification-service/naas/internal/config"
	"github.com/notification-service/naas/internal/preferences"
	"github.com/notification-service/naas/pkg/models"
	"go.uber.org/zap"
)

type Router struct {
	channelRegistry *channels.Registry
	preferenceManager *preferences.Manager
	logger          *zap.Logger
	config          *config.Config
	costMatrix      map[string]map[string]float64
	costMu          sync.RWMutex
}

type RoutingDecision struct {
	Channel     models.Channel
	Provider    string
	Priority    models.Priority
	Delay       time.Duration
	FallbackChain []FallbackStep
	Reason      string
}

type FallbackStep struct {
	Channel  models.Channel
	Provider string
	Delay    time.Duration
	Condition string
}

type ProviderCost struct {
	Provider string
	Channel  models.Channel
	CostPerMessage float64
	Region   string
	UpdatedAt time.Time
}

func NewRouter(channelRegistry *channels.Registry, preferenceManager *preferences.Manager, logger *zap.Logger) *Router {
	r := &Router{
		channelRegistry: channelRegistry,
		preferenceManager: preferenceManager,
		logger:          logger,
		costMatrix:      make(map[string]map[string]float64),
	}
	r.initDefaultCosts()
	return r
}

func (r *Router) initDefaultCosts() {
	r.costMatrix["email"] = map[string]float64{
		"sendgrid": 0.0005,
		"ses":      0.0001,
		"mailgun":  0.0008,
	}
	r.costMatrix["sms"] = map[string]float64{
		"twilio":  0.0075,
		"plivo":   0.0065,
		"vonage":  0.0070,
	}
	r.costMatrix["push"] = map[string]float64{
		"apns": 0.0,
		"fcm":  0.0,
	}
	r.costMatrix["voice"] = map[string]float64{
		"twilio": 0.0085,
		"vonage": 0.0075,
		"plivo":  0.0080,
	}
}

func (r *Router) Route(ctx context.Context, notification *models.Notification) (*RoutingDecision, error) {
	if optedOut, err := r.preferenceManager.IsOptedOut(ctx, notification.TenantID, notification.UserID, notification.Channel); err != nil {
		return nil, err
	} else if optedOut {
		return nil, fmt.Errorf("user opted out of %s", notification.Channel)
	}

	if enabled, err := r.preferenceManager.IsChannelEnabled(ctx, notification.TenantID, notification.UserID, notification.Channel); err != nil {
		return nil, err
	} else if !enabled {
		return nil, fmt.Errorf("channel %s disabled for user", notification.Channel)
	}

	if inQuietHours, err := r.preferenceManager.IsInQuietHours(ctx, notification.TenantID, notification.UserID); err != nil {
		return nil, err
	} else if inQuietHours && notification.Priority != models.PriorityCritical && notification.Priority != models.PriorityHigh {
		nextTime, _ := r.preferenceManager.GetNextAvailableTime(ctx, notification.TenantID, notification.UserID)
		return &RoutingDecision{
			Channel:  notification.Channel,
			Delay:    time.Until(nextTime),
			Reason:   "quiet_hours",
		}, nil
	}

	providers := r.channelRegistry.GetProviders(notification.Channel)
	if len(providers) == 0 {
		return nil, fmt.Errorf("no providers available for channel %s", notification.Channel)
	}

	provider := r.selectProvider(notification.Channel, providers, notification.TenantID)

	decision := &RoutingDecision{
		Channel:  notification.Channel,
		Provider: provider.Name(),
		Priority: notification.Priority,
		Reason:   "primary",
	}

	fallbacks := r.getFallbackChain(notification)
	if len(fallbacks) > 0 {
		decision.FallbackChain = fallbacks
	}

	return decision, nil
}

func (r *Router) selectProvider(channel models.Channel, providers []channels.Provider, tenantID string) channels.Provider {
	if len(providers) == 1 {
		return providers[0]
	}

	type scoredProvider struct {
		provider channels.Provider
		score    float64
	}

	var scored []scoredProvider
	for _, p := range providers {
		score := r.calculateProviderScore(channel, p.Name(), tenantID)
		scored = append(scored, scoredProvider{p, score})
	}

	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score < scored[j].score
	})

	return scored[0].provider
}

func (r *Router) calculateProviderScore(channel models.Channel, providerName, tenantID string) float64 {
	r.costMu.RLock()
	costs := r.costMatrix[string(channel)]
	r.costMu.RUnlock()

	cost := costs[providerName]
	if cost == 0 {
		cost = 1.0
	}

	cbState := r.getCircuitBreakerState(channel, providerName)
	if cbState == "open" {
		return math.MaxFloat64
	}

	if cbState == "half-open" {
		cost *= 1.5
	}

	return cost + rand.Float64()*0.001
}

func (r *Router) getCircuitBreakerState(channel models.Channel, providerName string) string {
	provider := r.channelRegistry.GetProvider(channel, providerName)
	if provider == nil {
		return "closed"
	}
	return "closed"
}

// SetConfig provides routing rules (fallback chains); without it only the built-in defaults apply.
func (r *Router) SetConfig(cfg *config.Config) {
	r.config = cfg
}

func (r *Router) getFallbackChain(notification *models.Notification) []FallbackStep {
	var fallbacks []FallbackStep

	var fallbackRules []config.FallbackRuleConfig
	if r.config != nil {
		fallbackRules = r.config.Routing.FallbackRules
	}

	for _, rule := range fallbackRules {
		if rule.FromChannel == string(notification.Channel) {
			fallbacks = append(fallbacks, FallbackStep{
				Channel:   models.Channel(rule.ToChannel),
				Delay:     rule.Delay,
				Condition: rule.Condition,
			})
		}
	}

	if len(fallbacks) == 0 {
		switch notification.Channel {
		case models.ChannelInApp:
			fallbacks = append(fallbacks, FallbackStep{
				Channel: models.ChannelPush,
				Delay:   5 * time.Minute,
				Condition: "unread",
			})
			fallbacks = append(fallbacks, FallbackStep{
				Channel: models.ChannelSMS,
				Delay:   15 * time.Minute,
				Condition: "unread",
			})
		case models.ChannelPush:
			fallbacks = append(fallbacks, FallbackStep{
				Channel: models.ChannelSMS,
				Delay:   10 * time.Minute,
				Condition: "failed",
			})
		case models.ChannelEmail:
			fallbacks = append(fallbacks, FallbackStep{
				Channel: models.ChannelSMS,
				Delay:   30 * time.Minute,
				Condition: "bounced",
			})
		}
	}

	return fallbacks
}

func (r *Router) UpdateCosts(channel models.Channel, costs map[string]float64) {
	r.costMu.Lock()
	defer r.costMu.Unlock()
	r.costMatrix[string(channel)] = costs
}

func (r *Router) GetCost(channel models.Channel, provider string) float64 {
	r.costMu.RLock()
	defer r.costMu.RUnlock()
	if costs, ok := r.costMatrix[string(channel)]; ok {
		return costs[provider]
	}
	return 0
}

func (r *Router) RouteForBulk(ctx context.Context, tenantID string, userIDs []string, channel models.Channel, priority models.Priority) (map[string]*RoutingDecision, error) {
	decisions := make(map[string]*RoutingDecision)

	for _, userID := range userIDs {
		notification := &models.Notification{
			TenantID: tenantID,
			UserID:   userID,
			Channel:  channel,
			Priority: priority,
		}

		decision, err := r.Route(ctx, notification)
		if err != nil {
			decisions[userID] = &RoutingDecision{
				Channel: channel,
				Reason:  "error: " + err.Error(),
			}
			continue
		}
		decisions[userID] = decision
	}

	return decisions, nil
}

func (r *Router) GetAvailableChannels(ctx context.Context, tenantID, userID string) ([]models.Channel, error) {
	var available []models.Channel

	for _, ch := range []models.Channel{
		models.ChannelEmail, models.ChannelSMS, models.ChannelPush,
		models.ChannelInApp, models.ChannelWebhook, models.ChannelVoice,
	} {
		enabled, err := r.preferenceManager.IsChannelEnabled(ctx, tenantID, userID, ch)
		if err != nil {
			continue
		}
		if !enabled {
			continue
		}

		optedOut, err := r.preferenceManager.IsOptedOut(ctx, tenantID, userID, ch)
		if err != nil || optedOut {
			continue
		}

		providers := r.channelRegistry.GetProviders(ch)
		if len(providers) > 0 {
			available = append(available, ch)
		}
	}

	return available, nil
}