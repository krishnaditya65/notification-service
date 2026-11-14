package orchestration

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/notification-service/naas/internal/channels"
	"github.com/notification-service/naas/internal/preferences"
	"github.com/notification-service/naas/internal/processing"
	"github.com/notification-service/naas/internal/reliability"
	"github.com/notification-service/naas/internal/routing"
	"github.com/notification-service/naas/internal/templates"
	"github.com/notification-service/naas/pkg/errors"
	"github.com/notification-service/naas/pkg/models"
	"go.uber.org/zap"
)

type Orchestrator struct {
	router           *routing.Router
	templateEngine   *templates.Engine
	preferenceManager *preferences.Manager
	channelRegistry  *channels.Registry
	reliabilityMgr   *reliability.Manager
	recorder         NotificationRecorder
	logger           *zap.Logger
}

// NotificationRecorder persists the lifecycle of every notification for the console and audits.
type NotificationRecorder interface {
	Save(ctx context.Context, notification *models.Notification) error
}

type PreferencesManager interface {
	GetPreferences(ctx context.Context, tenantID, userID string) (*models.UserPreferences, error)
	IsOptedOut(ctx context.Context, tenantID, userID string, channel models.Channel) (bool, error)
	IsChannelEnabled(ctx context.Context, tenantID, userID string, channel models.Channel) (bool, error)
	IsInQuietHours(ctx context.Context, tenantID, userID string) (bool, error)
	GetNextAvailableTime(ctx context.Context, tenantID, userID string) (time.Time, error)
	CheckFrequencyCap(ctx context.Context, tenantID, userID, category string) (bool, error)
	IncrementFrequencyCap(ctx context.Context, tenantID, userID, category string) error
}

type ProcessorInterface interface {
	ProcessBulk(ctx context.Context, job *processing.BulkJob) error
	AddToDigest(ctx context.Context, event *models.Event, digestKey string, window time.Duration) error
	GenerateIdempotencyKey(tenantID, userID, templateID, channel string, variables map[string]interface{}) string
}

func NewOrchestrator(
	router *routing.Router,
	templateEngine *templates.Engine,
	preferenceManager *preferences.Manager,
	logger *zap.Logger,
) *Orchestrator {
	return &Orchestrator{
		router:            router,
		templateEngine:    templateEngine,
		preferenceManager: preferenceManager,
		logger:            logger,
	}
}

func (o *Orchestrator) SetChannelRegistry(registry *channels.Registry) {
	o.channelRegistry = registry
}

func (o *Orchestrator) SetNotificationRecorder(recorder NotificationRecorder) {
	o.recorder = recorder
}

const defaultMaxRetries = 3

// prepare fills identifiers and defaults a caller may omit, so every notification is trackable.
func prepareNotification(notification *models.Notification) {
	if notification.ID == "" {
		notification.ID = models.NewID("ntf")
	}

	if notification.Payload == nil {
		notification.Payload = map[string]interface{}{}
	}

	if notification.MaxRetries == 0 {
		notification.MaxRetries = defaultMaxRetries
	}

	if notification.Status == "" {
		notification.Status = models.StatusPending
	}

	if notification.CreatedAt.IsZero() {
		notification.CreatedAt = time.Now()
	}
}

var suppressionReasons = []string{"opted out", "disabled for user"}

func statusForFailure(err error) models.Status {
	for _, reason := range suppressionReasons {
		if strings.Contains(err.Error(), reason) {
			return models.StatusSuppressed
		}
	}

	return models.StatusFailed
}

// finish records the final state of an Execute call.
func (o *Orchestrator) finish(ctx context.Context, notification *models.Notification, executeErr error) {
	if executeErr != nil {
		notification.Status = statusForFailure(executeErr)
		notification.Error = executeErr.Error()
	}

	if o.recorder == nil {
		return
	}

	if err := o.recorder.Save(context.WithoutCancel(ctx), notification); err != nil {
		o.logger.Error("Failed to persist notification", zap.String("notification_id", notification.ID), zap.Error(err))
	}
}

func (o *Orchestrator) SetReliabilityManager(mgr *reliability.Manager) {
	o.reliabilityMgr = mgr
}

func (o *Orchestrator) Execute(ctx context.Context, notification *models.Notification, processor interface{}) (executeErr error) {
	prepareNotification(notification)
	defer func() { o.finish(ctx, notification, executeErr) }()

	var proc ProcessorInterface
	if processor != nil {
		if p, ok := processor.(ProcessorInterface); ok {
			proc = p
		}
	}
	startTime := time.Now()
	o.emitEvent(ctx, notification, "enqueued", nil)

	if notification.IdempotencyKey == "" && proc != nil {
		notification.IdempotencyKey = proc.GenerateIdempotencyKey(
			notification.TenantID, notification.UserID, notification.TemplateID,
			string(notification.Channel), notification.Variables,
		)
	}

	decision, err := o.router.Route(ctx, notification)
	if err != nil {
		o.emitEvent(ctx, notification, "routing_failed", map[string]interface{}{"error": err.Error()})
		return errors.Wrap(err, errors.ErrInternalError.Error(), "routing failed", nil)
	}

	if decision.Delay > 0 {
		notification.ScheduledAt = &[]time.Time{time.Now().Add(decision.Delay)}[0]
		notification.Status = models.StatusQueued
		o.emitEvent(ctx, notification, "scheduled", map[string]interface{}{"delay": decision.Delay.String()})
		return o.scheduleNotification(ctx, notification, decision.Delay)
	}

	notification.Status = models.StatusRouted
	o.emitEvent(ctx, notification, "routed", map[string]interface{}{
		"channel": decision.Channel,
		"provider": decision.Provider,
	})

	if err := o.sendNotification(ctx, notification, decision); err != nil {
		o.emitEvent(ctx, notification, "send_failed", map[string]interface{}{"error": err.Error()})
		
		if o.reliabilityMgr != nil {
			if retryErr := o.reliabilityMgr.ScheduleRetry(ctx, notification, err); retryErr != nil {
				o.logger.Error("Failed to schedule retry", zap.Error(retryErr))
			}
		}
		return err
	}

	notification.Status = models.StatusSent
	now := time.Now()
	notification.SentAt = &now
	o.emitEvent(ctx, notification, "sent", map[string]interface{}{
		"provider": decision.Provider,
		"latency_ms": time.Since(startTime).Milliseconds(),
	})

	if len(decision.FallbackChain) > 0 {
		go o.executeFallbackChain(context.Background(), notification, decision.FallbackChain, proc)
	}

	return nil
}

func (o *Orchestrator) sendNotification(ctx context.Context, notification *models.Notification, decision *routing.RoutingDecision) error {
	if o.reliabilityMgr != nil {
		if !o.reliabilityMgr.AllowSend(notification.Channel) {
			wait := o.reliabilityMgr.WaitForCapacity(notification.Channel)
			if wait > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(wait):
				}
			}
		}
	}

	provider := o.channelRegistry.GetProvider(decision.Channel, decision.Provider)
	if provider == nil {
		return errors.NewAppError("PROVIDER_NOT_FOUND", "provider not found", nil, map[string]interface{}{
			"channel": decision.Channel,
			"provider": decision.Provider,
		})
	}

	message := &channels.Message{
		ID:        notification.ID,
		TenantID:  notification.TenantID,
		UserID:    notification.UserID,
		To:        o.getRecipientAddress(notification),
		Subject:   stringFromPayload(notification.Payload, "subject"),
		Body:      stringFromPayload(notification.Payload, "body"),
		Variables: notification.Variables,
		Metadata:  notification.Metadata,
		Priority:  notification.Priority,
		Locale:    notification.Locale,
	}

	if message.Subject == "" || message.Body == "" {
		if notification.TemplateID != "" {
			subject, body, err := o.templateEngine.Render(ctx, notification.TenantID, notification.TemplateID, notification.Locale, notification.Variables)
			if err != nil {
				return errors.Wrap(err, errors.ErrTemplateRenderFailed.Error(), "failed to render template", nil)
			}
			message.Subject = subject
			message.Body = body
			notification.Payload["subject"], notification.Payload["body"] = subject, body
		}
	}

	result, err := provider.Send(ctx, message)
	if err != nil {
		return errors.Wrap(err, errors.ErrProviderError.Error(), "provider send failed", map[string]interface{}{
			"provider": decision.Provider,
		})
	}

	notification.Provider = decision.Provider
	notification.ProviderID = result.ProviderMsgID
	notification.Metadata = result.Metadata

	return nil
}

func stringFromPayload(payload map[string]interface{}, key string) string {
	value, _ := payload[key].(string)
	return value
}

func (o *Orchestrator) getRecipientAddress(notification *models.Notification) string {
	if addr, ok := notification.Metadata["recipient"].(string); ok {
		return addr
	}
	if notification.Channel == models.ChannelEmail {
		return notification.UserID + "@example.com"
	}
	if notification.Channel == models.ChannelSMS || notification.Channel == models.ChannelVoice {
		return notification.UserID
	}
	if notification.Channel == models.ChannelPush {
		token, _ := notification.Metadata["device_token"].(string)
		return token
	}
	return notification.UserID
}

func (o *Orchestrator) scheduleNotification(ctx context.Context, notification *models.Notification, delay time.Duration) error {
	time.AfterFunc(delay, func() {
		newCtx := context.Background()
		if err := o.Execute(newCtx, notification, nil); err != nil {
			o.logger.Error("Scheduled notification failed", zap.String("notification_id", notification.ID), zap.Error(err))
		}
	})
	return nil
}

func (o *Orchestrator) executeFallbackChain(ctx context.Context, original *models.Notification, fallbacks []routing.FallbackStep, processor ProcessorInterface) {
	for _, step := range fallbacks {
		select {
		case <-ctx.Done():
			return
		case <-time.After(step.Delay):
		}

		if step.Condition == "unread" || step.Condition == "failed" || step.Condition == "bounced" {
			fallbackNotification := &models.Notification{
				ID:          fmt.Sprintf("%s-fallback-%s", original.ID, step.Channel),
				TenantID:    original.TenantID,
				UserID:      original.UserID,
				TemplateID:  original.TemplateID,
				Channel:     step.Channel,
				Priority:    original.Priority,
				Variables:   original.Variables,
				Metadata:    original.Metadata,
				Locale:      original.Locale,
			}

			if err := o.Execute(ctx, fallbackNotification, processor); err != nil {
				o.logger.Error("Fallback failed", zap.String("original_id", original.ID), zap.String("fallback_channel", string(step.Channel)), zap.Error(err))
			} else {
				o.logger.Info("Fallback succeeded", zap.String("original_id", original.ID), zap.String("fallback_channel", string(step.Channel)))
				break
			}
		}
	}
}

func (o *Orchestrator) emitEvent(ctx context.Context, notification *models.Notification, eventType string, metadata map[string]interface{}) {
	event := &models.Event{
		ID:        fmt.Sprintf("evt-%s-%d", notification.ID, time.Now().UnixNano()),
		Type:      eventType,
		TenantID:  notification.TenantID,
		UserID:    notification.UserID,
		Payload: map[string]interface{}{
			"notification_id": notification.ID,
			"channel":         notification.Channel,
			"priority":        notification.Priority,
			"status":          notification.Status,
		},
		Timestamp: time.Now(),
		Metadata:  metadata,
	}

	if metadata != nil {
		for k, v := range metadata {
			event.Payload[k] = v
		}
	}

	o.logger.Debug("Event emitted", zap.String("type", eventType), zap.String("notification_id", notification.ID))
}

func (o *Orchestrator) ExecuteBulk(ctx context.Context, job *processing.BulkJob, processor ProcessorInterface) error {
	return processor.ProcessBulk(ctx, job)
}

func (o *Orchestrator) AddToDigest(ctx context.Context, event *models.Event, digestKey string, window time.Duration, processor ProcessorInterface) error {
	return processor.AddToDigest(ctx, event, digestKey, window)
}
// ProviderHealth is the live state of one configured channel provider.
type ProviderHealth struct {
	Channel  models.Channel `json:"channel"`
	Name     string         `json:"name"`
	Healthy  bool           `json:"healthy"`
	Detail   string         `json:"detail,omitempty"`
}

var allChannels = []models.Channel{models.ChannelEmail, models.ChannelSMS, models.ChannelPush, models.ChannelInApp, models.ChannelWebhook, models.ChannelVoice}

// ProviderHealth probes every registered provider.
func (o *Orchestrator) ProviderHealth(ctx context.Context) []ProviderHealth {
	health := []ProviderHealth{}

	if o.channelRegistry == nil {
		return health
	}

	for _, channel := range allChannels {
		for _, provider := range o.channelRegistry.GetProviders(channel) {
			entry := ProviderHealth{Channel: channel, Name: provider.Name(), Healthy: true}

			if err := provider.HealthCheck(ctx); err != nil {
				entry.Healthy, entry.Detail = false, err.Error()
			}

			health = append(health, entry)
		}
	}

	return health
}
