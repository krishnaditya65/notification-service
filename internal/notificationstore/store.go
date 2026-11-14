// Package notificationstore persists notifications and answers the console's list and statistics queries.
package notificationstore

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/notification-service/naas/pkg/models"
)

//go:embed schema.sql
var schemaSQL string

const (
	DefaultPageSize = 50
	MaximumPageSize = 200
)

type Store struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// EnsureSchema creates every table the service uses; safe to run on each start.
func (s *Store) EnsureSchema(ctx context.Context) error {
	_, err := s.db.Exec(ctx, schemaSQL)
	return err
}

func jsonOrEmptyObject(value map[string]interface{}) []byte {
	if value == nil {
		return []byte("{}")
	}

	raw, _ := json.Marshal(value)

	return raw
}

// Save inserts or updates a notification keyed by its ID.
func (s *Store) Save(ctx context.Context, notification *models.Notification) error {
	now := time.Now()
	if notification.CreatedAt.IsZero() {
		notification.CreatedAt = now
	}

	notification.UpdatedAt = now

	_, err := s.db.Exec(ctx, `
		INSERT INTO notifications (id, tenant_id, user_id, template_id, channel, priority, status, payload, variables, locale,
			scheduled_at, sent_at, delivered_at, opened_at, clicked_at, error, retry_count, max_retries, provider, provider_id,
			idempotency_key, metadata, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
		ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, payload = EXCLUDED.payload, scheduled_at = EXCLUDED.scheduled_at,
			sent_at = EXCLUDED.sent_at, delivered_at = EXCLUDED.delivered_at, opened_at = EXCLUDED.opened_at,
			clicked_at = EXCLUDED.clicked_at, error = EXCLUDED.error, retry_count = EXCLUDED.retry_count,
			provider = EXCLUDED.provider, provider_id = EXCLUDED.provider_id, metadata = EXCLUDED.metadata, updated_at = EXCLUDED.updated_at`,
		notification.ID, notification.TenantID, notification.UserID, notification.TemplateID, string(notification.Channel), string(notification.Priority),
		string(notification.Status), jsonOrEmptyObject(notification.Payload), jsonOrEmptyObject(notification.Variables), notification.Locale,
		notification.ScheduledAt, notification.SentAt, notification.DeliveredAt, notification.OpenedAt, notification.ClickedAt,
		notification.Error, notification.RetryCount, notification.MaxRetries, notification.Provider, notification.ProviderID,
		notification.IdempotencyKey, jsonOrEmptyObject(notification.Metadata), notification.CreatedAt, notification.UpdatedAt)

	return err
}

const notificationColumns = `id, tenant_id, user_id, template_id, channel, priority, status, payload, variables, locale, scheduled_at, sent_at,
	delivered_at, opened_at, clicked_at, error, retry_count, max_retries, provider, provider_id, idempotency_key, metadata, created_at, updated_at`

func scanNotification(row pgx.Row) (*models.Notification, error) {
	var notification models.Notification
	var channel, priority, status string
	var payload, variables, metadata []byte

	err := row.Scan(&notification.ID, &notification.TenantID, &notification.UserID, &notification.TemplateID, &channel, &priority, &status,
		&payload, &variables, &notification.Locale, &notification.ScheduledAt, &notification.SentAt, &notification.DeliveredAt,
		&notification.OpenedAt, &notification.ClickedAt, &notification.Error, &notification.RetryCount, &notification.MaxRetries,
		&notification.Provider, &notification.ProviderID, &notification.IdempotencyKey, &metadata, &notification.CreatedAt, &notification.UpdatedAt)
	if err != nil {
		return nil, err
	}

	notification.Channel, notification.Priority, notification.Status = models.Channel(channel), models.Priority(priority), models.Status(status)
	_ = json.Unmarshal(payload, &notification.Payload)
	_ = json.Unmarshal(variables, &notification.Variables)
	_ = json.Unmarshal(metadata, &notification.Metadata)

	return &notification, nil
}

func (s *Store) Get(ctx context.Context, id string) (*models.Notification, error) {
	return scanNotification(s.db.QueryRow(ctx, `SELECT `+notificationColumns+` FROM notifications WHERE id = $1`, id))
}

// Filter narrows List; empty fields are ignored.
type Filter struct {
	TenantID, Status, Channel, Provider, UserID, Text string
	Limit, Offset                                     int
}

type ListResult struct {
	Notifications []*models.Notification `json:"notifications"`
	HasMore       bool                   `json:"has_more"`
}

func (f Filter) whereClause() (string, []interface{}) {
	conditions, args := []string{"TRUE"}, []interface{}{}
	add := func(column, value string) {
		if value != "" {
			args = append(args, value)
			conditions = append(conditions, fmt.Sprintf("%s = $%d", column, len(args)))
		}
	}

	add("tenant_id", f.TenantID)
	add("status", f.Status)
	add("channel", f.Channel)
	add("provider", f.Provider)
	add("user_id", f.UserID)

	if f.Text != "" {
		args = append(args, f.Text, "%"+escapeLike(f.Text)+"%")
		conditions = append(conditions, fmt.Sprintf("(id = $%d OR template_id ILIKE $%d OR user_id ILIKE $%d)", len(args)-1, len(args), len(args)))
	}

	return strings.Join(conditions, " AND "), args
}

func escapeLike(text string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(text)
}

func (s *Store) List(ctx context.Context, filter Filter) (ListResult, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = DefaultPageSize
	}

	limit = min(limit, MaximumPageSize)
	where, args := filter.whereClause()
	args = append(args, limit+1, max(filter.Offset, 0))

	rows, err := s.db.Query(ctx, fmt.Sprintf(`SELECT %s FROM notifications WHERE %s ORDER BY created_at DESC LIMIT $%d OFFSET $%d`,
		notificationColumns, where, len(args)-1, len(args)), args...)
	if err != nil {
		return ListResult{}, err
	}
	defer rows.Close()

	result := ListResult{Notifications: []*models.Notification{}}

	for rows.Next() {
		notification, err := scanNotification(rows)
		if err != nil {
			return ListResult{}, err
		}

		result.Notifications = append(result.Notifications, notification)
	}

	if len(result.Notifications) > limit {
		result.Notifications, result.HasMore = result.Notifications[:limit], true
	}

	return result, rows.Err()
}

type Count struct {
	Key   string `json:"key"`
	Total int64  `json:"total"`
}

type HourlyPoint struct {
	Hour      time.Time `json:"hour"`
	Delivered int64     `json:"delivered"`
	Failed    int64     `json:"failed"`
	Other     int64     `json:"other"`
}

type Overview struct {
	ByStatus        []Count       `json:"by_status"`
	ByChannel       []Count       `json:"by_channel"`
	Hourly          []HourlyPoint `json:"hourly"`
	P95LatencyMs    *float64      `json:"p95_latency_ms"`
	DeadLetterCount int64         `json:"dead_letter_count"`
}

var (
	deliveredStatuses = "'sent','delivered','opened','clicked'"
	failedStatuses    = "'failed','bounced'"
)

func (s *Store) Overview(ctx context.Context, tenantID string, since time.Time) (Overview, error) {
	var overview Overview
	var err error

	if overview.ByStatus, err = s.counts(ctx, "status", tenantID, since); err != nil {
		return overview, err
	}

	if overview.ByChannel, err = s.counts(ctx, "channel", tenantID, since); err != nil {
		return overview, err
	}

	rows, err := s.db.Query(ctx, fmt.Sprintf(`
		SELECT date_trunc('hour', created_at) AS hour,
		       COUNT(*) FILTER (WHERE status IN (%s)), COUNT(*) FILTER (WHERE status IN (%s)),
		       COUNT(*) FILTER (WHERE status NOT IN (%s) AND status NOT IN (%s))
		FROM notifications WHERE created_at >= $1 AND ($2 = '' OR tenant_id = $2) GROUP BY 1 ORDER BY 1`,
		deliveredStatuses, failedStatuses, deliveredStatuses, failedStatuses), since, tenantID)
	if err != nil {
		return overview, err
	}

	defer rows.Close()

	for rows.Next() {
		var point HourlyPoint
		if err := rows.Scan(&point.Hour, &point.Delivered, &point.Failed, &point.Other); err != nil {
			return overview, err
		}

		overview.Hourly = append(overview.Hourly, point)
	}

	if err := rows.Err(); err != nil {
		return overview, err
	}

	err = s.db.QueryRow(ctx, `
		SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (sent_at - created_at)) * 1000)::double precision
		FROM notifications WHERE sent_at IS NOT NULL AND created_at >= $1 AND ($2 = '' OR tenant_id = $2)`, since, tenantID).Scan(&overview.P95LatencyMs)
	if err != nil {
		return overview, err
	}

	err = s.db.QueryRow(ctx, `SELECT COUNT(*) FROM dead_letter_queue WHERE $1 = '' OR tenant_id = $1`, tenantID).Scan(&overview.DeadLetterCount)

	return overview, err
}

func (s *Store) counts(ctx context.Context, column, tenantID string, since time.Time) ([]Count, error) {
	rows, err := s.db.Query(ctx, fmt.Sprintf(`SELECT %s, COUNT(*) FROM notifications WHERE created_at >= $1 AND ($2 = '' OR tenant_id = $2) GROUP BY 1 ORDER BY 2 DESC`, column), since, tenantID)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	counts := []Count{}

	for rows.Next() {
		var count Count
		if err := rows.Scan(&count.Key, &count.Total); err != nil {
			return nil, err
		}

		counts = append(counts, count)
	}

	return counts, rows.Err()
}

type ProviderStats struct {
	Channel      string   `json:"channel"`
	Provider     string   `json:"provider"`
	Total        int64    `json:"total"`
	Failed       int64    `json:"failed"`
	AvgLatencyMs *float64 `json:"avg_latency_ms"`
}

func (s *Store) ProviderBreakdown(ctx context.Context, since time.Time) ([]ProviderStats, error) {
	rows, err := s.db.Query(ctx, fmt.Sprintf(`
		SELECT channel, provider, COUNT(*), COUNT(*) FILTER (WHERE status IN (%s)),
		       AVG(EXTRACT(EPOCH FROM (sent_at - created_at)) * 1000)::double precision
		FROM notifications WHERE created_at >= $1 GROUP BY 1, 2 ORDER BY 3 DESC`, failedStatuses), since)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	stats := []ProviderStats{}

	for rows.Next() {
		var row ProviderStats
		if err := rows.Scan(&row.Channel, &row.Provider, &row.Total, &row.Failed, &row.AvgLatencyMs); err != nil {
			return nil, err
		}

		stats = append(stats, row)
	}

	return stats, rows.Err()
}

func (s *Store) Tenants(ctx context.Context) ([]Count, error) {
	rows, err := s.db.Query(ctx, `SELECT tenant_id, COUNT(*) FROM notifications GROUP BY 1 ORDER BY 2 DESC LIMIT 200`)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	tenants := []Count{}

	for rows.Next() {
		var tenant Count
		if err := rows.Scan(&tenant.Key, &tenant.Total); err != nil {
			return nil, err
		}

		tenants = append(tenants, tenant)
	}

	return tenants, rows.Err()
}
