package preferences

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/notification-service/naas/pkg/models"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Manager struct {
	db    *pgxpool.Pool
	redis *redis.Client
	logger *zap.Logger
}

func NewManager(db *pgxpool.Pool, redisClient *redis.Client, logger *zap.Logger) *Manager {
	return &Manager{
		db:    db,
		redis: redisClient,
		logger: logger,
	}
}

func (m *Manager) GetPreferences(ctx context.Context, tenantID, userID string) (*models.UserPreferences, error) {
	cacheKey := fmt.Sprintf("prefs:%s:%s", tenantID, userID)

	cached, err := m.redis.Get(ctx, cacheKey).Bytes()
	if err == nil {
		var prefs models.UserPreferences
		if json.Unmarshal(cached, &prefs) == nil {
			return &prefs, nil
		}
	}

	query := `
		SELECT user_id, tenant_id, channels, categories, quiet_hours, timezone, locale, frequency_caps, updated_at
		FROM user_preferences
		WHERE tenant_id = $1 AND user_id = $2
	`

	var prefs models.UserPreferences
	var channelsJSON, categoriesJSON, quietHoursJSON, freqCapsJSON []byte

	err = m.db.QueryRow(ctx, query, tenantID, userID).Scan(
		&prefs.UserID, &prefs.TenantID, &channelsJSON, &categoriesJSON,
		&quietHoursJSON, &prefs.Timezone, &prefs.Locale, &freqCapsJSON, &prefs.UpdatedAt,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return m.getDefaultPreferences(tenantID, userID), nil
		}
		return nil, err
	}

	json.Unmarshal(channelsJSON, &prefs.Channels)
	json.Unmarshal(categoriesJSON, &prefs.Categories)
	json.Unmarshal(quietHoursJSON, &prefs.QuietHours)
	json.Unmarshal(freqCapsJSON, &prefs.FrequencyCaps)

	m.cachePreferences(ctx, cacheKey, &prefs)
	return &prefs, nil
}

func (m *Manager) getDefaultPreferences(tenantID, userID string) *models.UserPreferences {
	channels := make(map[models.Channel]models.ChannelPref)
	for _, ch := range []models.Channel{models.ChannelEmail, models.ChannelSMS, models.ChannelPush, models.ChannelInApp, models.ChannelWebhook, models.ChannelVoice} {
		channels[ch] = models.ChannelPref{Enabled: true, OptedOut: false}
	}

	return &models.UserPreferences{
		UserID:     userID,
		TenantID:   tenantID,
		Channels:   channels,
		Categories: make(map[string]models.CategoryPref),
		QuietHours: models.QuietHours{Enabled: false, StartHour: 22, EndHour: 8, Timezone: "UTC"},
		Timezone:   "UTC",
		Locale:     "en",
		FrequencyCaps: make(map[string]models.FrequencyCap),
		UpdatedAt:  time.Now(),
	}
}

func (m *Manager) cachePreferences(ctx context.Context, key string, prefs *models.UserPreferences) {
	data, _ := json.Marshal(prefs)
	m.redis.Set(ctx, key, data, 10*time.Minute)
}

func (m *Manager) UpsertPreferences(ctx context.Context, prefs *models.UserPreferences) error {
	prefs.UpdatedAt = time.Now()

	channelsJSON, _ := json.Marshal(prefs.Channels)
	categoriesJSON, _ := json.Marshal(prefs.Categories)
	quietHoursJSON, _ := json.Marshal(prefs.QuietHours)
	freqCapsJSON, _ := json.Marshal(prefs.FrequencyCaps)

	query := `
		INSERT INTO user_preferences (user_id, tenant_id, channels, categories, quiet_hours, timezone, locale, frequency_caps, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (tenant_id, user_id) DO UPDATE SET
			channels = EXCLUDED.channels,
			categories = EXCLUDED.categories,
			quiet_hours = EXCLUDED.quiet_hours,
			timezone = EXCLUDED.timezone,
			locale = EXCLUDED.locale,
			frequency_caps = EXCLUDED.frequency_caps,
			updated_at = EXCLUDED.updated_at
	`

	_, err := m.db.Exec(ctx, query,
		prefs.UserID, prefs.TenantID, channelsJSON, categoriesJSON,
		quietHoursJSON, prefs.Timezone, prefs.Locale, freqCapsJSON, prefs.UpdatedAt,
	)

	if err != nil {
		return err
	}

	m.invalidateCache(ctx, prefs.TenantID, prefs.UserID)
	return nil
}

func (m *Manager) invalidateCache(ctx context.Context, tenantID, userID string) {
	key := fmt.Sprintf("prefs:%s:%s", tenantID, userID)
	m.redis.Del(ctx, key)
}

func (m *Manager) IsOptedOut(ctx context.Context, tenantID, userID string, channel models.Channel) (bool, error) {
	prefs, err := m.GetPreferences(ctx, tenantID, userID)
	if err != nil {
		return false, err
	}

	if chPref, ok := prefs.Channels[channel]; ok {
		return chPref.OptedOut, nil
	}
	return false, nil
}

func (m *Manager) IsChannelEnabled(ctx context.Context, tenantID, userID string, channel models.Channel) (bool, error) {
	prefs, err := m.GetPreferences(ctx, tenantID, userID)
	if err != nil {
		return false, err
	}

	if chPref, ok := prefs.Channels[channel]; ok {
		return chPref.Enabled, nil
	}
	return true, nil
}

func (m *Manager) IsInQuietHours(ctx context.Context, tenantID, userID string) (bool, error) {
	prefs, err := m.GetPreferences(ctx, tenantID, userID)
	if err != nil {
		return false, err
	}

	if !prefs.QuietHours.Enabled {
		return false, nil
	}

	loc, err := time.LoadLocation(prefs.QuietHours.Timezone)
	if err != nil {
		loc = time.UTC
	}

	now := time.Now().In(loc)
	currentHour := now.Hour()

	start := prefs.QuietHours.StartHour
	end := prefs.QuietHours.EndHour

	if start > end {
		return currentHour >= start || currentHour < end, nil
	}
	return currentHour >= start && currentHour < end, nil
}

func (m *Manager) GetNextAvailableTime(ctx context.Context, tenantID, userID string) (time.Time, error) {
	prefs, err := m.GetPreferences(ctx, tenantID, userID)
	if err != nil {
		return time.Time{}, err
	}

	if !prefs.QuietHours.Enabled {
		return time.Now(), nil
	}

	loc, err := time.LoadLocation(prefs.QuietHours.Timezone)
	if err != nil {
		loc = time.UTC
	}

	now := time.Now().In(loc)
	currentHour := now.Hour()

	if currentHour < prefs.QuietHours.StartHour && currentHour >= prefs.QuietHours.EndHour {
		return now, nil
	}

	nextDay := now.AddDate(0, 0, 1)
	nextAvailable := time.Date(nextDay.Year(), nextDay.Month(), nextDay.Day(), prefs.QuietHours.EndHour, 0, 0, 0, loc)

	return nextAvailable, nil
}

func (m *Manager) CheckFrequencyCap(ctx context.Context, tenantID, userID, category string) (bool, error) {
	prefs, err := m.GetPreferences(ctx, tenantID, userID)
	if err != nil {
		return false, err
	}

	capKey := category
	if capKey == "" {
		capKey = "default"
	}

	freqCap, ok := prefs.FrequencyCaps[capKey]
	if !ok {
		return true, nil
	}

	now := time.Now()
	if now.Sub(freqCap.WindowStart) > freqCap.Window {
		freqCap.CurrentCount = 0
		freqCap.WindowStart = now
	}

	if freqCap.CurrentCount >= freqCap.MaxCount {
		return false, nil
	}

	freqCap.CurrentCount++
	prefs.FrequencyCaps[capKey] = freqCap

	return true, m.UpsertPreferences(ctx, prefs)
}

func (m *Manager) IncrementFrequencyCap(ctx context.Context, tenantID, userID, category string) error {
	prefs, err := m.GetPreferences(ctx, tenantID, userID)
	if err != nil {
		return err
	}

	capKey := category
	if capKey == "" {
		capKey = "default"
	}

	freqCap, ok := prefs.FrequencyCaps[capKey]
	if !ok {
		return nil
	}

	now := time.Now()
	if now.Sub(freqCap.WindowStart) > freqCap.Window {
		freqCap.CurrentCount = 1
		freqCap.WindowStart = now
	} else {
		freqCap.CurrentCount++
	}

	prefs.FrequencyCaps[capKey] = freqCap
	return m.UpsertPreferences(ctx, prefs)
}

func (m *Manager) SetFrequencyCap(ctx context.Context, tenantID, userID, category string, maxCount int, window time.Duration) error {
	prefs, err := m.GetPreferences(ctx, tenantID, userID)
	if err != nil {
		return err
	}

	capKey := category
	if capKey == "" {
		capKey = "default"
	}

	prefs.FrequencyCaps[capKey] = models.FrequencyCap{
		MaxCount:    maxCount,
		Window:      window,
		CurrentCount: 0,
		WindowStart: time.Now(),
	}

	return m.UpsertPreferences(ctx, prefs)
}

func (m *Manager) OptOut(ctx context.Context, tenantID, userID string, channel models.Channel) error {
	prefs, err := m.GetPreferences(ctx, tenantID, userID)
	if err != nil {
		return err
	}

	if chPref, ok := prefs.Channels[channel]; ok {
		chPref.OptedOut = true
		chPref.Enabled = false
		prefs.Channels[channel] = chPref
	}

	return m.UpsertPreferences(ctx, prefs)
}

func (m *Manager) OptIn(ctx context.Context, tenantID, userID string, channel models.Channel) error {
	prefs, err := m.GetPreferences(ctx, tenantID, userID)
	if err != nil {
		return err
	}

	if chPref, ok := prefs.Channels[channel]; ok {
		chPref.OptedOut = false
		chPref.Enabled = true
		prefs.Channels[channel] = chPref
	}

	return m.UpsertPreferences(ctx, prefs)
}

func (m *Manager) SetQuietHours(ctx context.Context, tenantID, userID string, enabled bool, startHour, endHour int, timezone string) error {
	prefs, err := m.GetPreferences(ctx, tenantID, userID)
	if err != nil {
		return err
	}

	prefs.QuietHours = models.QuietHours{
		Enabled:   enabled,
		StartHour: startHour,
		EndHour:   endHour,
		Timezone:  timezone,
	}

	return m.UpsertPreferences(ctx, prefs)
}