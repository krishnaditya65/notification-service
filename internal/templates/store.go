package templates

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/notification-service/naas/pkg/models"
)

type PostgresStore struct {
	db *pgxpool.Pool
}

func NewPostgresStore(db *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) GetTemplate(ctx context.Context, tenantID, templateID string) (*models.Template, error) {
	query := `
		SELECT id, tenant_id, name, channel, subject, body, variables, locales, version, is_active, created_at, updated_at
		FROM templates
		WHERE id = $1 AND tenant_id = $2
	`

	var tmpl models.Template
	var localesJSON []byte
	var variablesJSON []byte

	err := s.db.QueryRow(ctx, query, templateID, tenantID).Scan(
		&tmpl.ID, &tmpl.TenantID, &tmpl.Name, &tmpl.Channel,
		&tmpl.Subject, &tmpl.Body, &variablesJSON, &localesJSON,
		&tmpl.Version, &tmpl.IsActive, &tmpl.CreatedAt, &tmpl.UpdatedAt,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, models.ErrTemplateNotFound
		}
		return nil, err
	}

	json.Unmarshal(variablesJSON, &tmpl.Variables)
	json.Unmarshal(localesJSON, &tmpl.Locales)

	return &tmpl, nil
}

func (s *PostgresStore) GetTemplateByName(ctx context.Context, tenantID, name, channel string) (*models.Template, error) {
	query := `
		SELECT id, tenant_id, name, channel, subject, body, variables, locales, version, is_active, created_at, updated_at
		FROM templates
		WHERE tenant_id = $1 AND name = $2 AND channel = $3 AND is_active = true
		ORDER BY version DESC
		LIMIT 1
	`

	var tmpl models.Template
	var localesJSON []byte
	var variablesJSON []byte

	err := s.db.QueryRow(ctx, query, tenantID, name, channel).Scan(
		&tmpl.ID, &tmpl.TenantID, &tmpl.Name, &tmpl.Channel,
		&tmpl.Subject, &tmpl.Body, &variablesJSON, &localesJSON,
		&tmpl.Version, &tmpl.IsActive, &tmpl.CreatedAt, &tmpl.UpdatedAt,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, models.ErrTemplateNotFound
		}
		return nil, err
	}

	json.Unmarshal(variablesJSON, &tmpl.Variables)
	json.Unmarshal(localesJSON, &tmpl.Locales)

	return &tmpl, nil
}

func (s *PostgresStore) CreateTemplate(ctx context.Context, tmpl *models.Template) error {
	variablesJSON, _ := json.Marshal(tmpl.Variables)
	localesJSON, _ := json.Marshal(tmpl.Locales)

	query := `
		INSERT INTO templates (id, tenant_id, name, channel, subject, body, variables, locales, version, is_active, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`

	_, err := s.db.Exec(ctx, query,
		tmpl.ID, tmpl.TenantID, tmpl.Name, tmpl.Channel,
		tmpl.Subject, tmpl.Body, variablesJSON, localesJSON,
		tmpl.Version, tmpl.IsActive, tmpl.CreatedAt, tmpl.UpdatedAt,
	)

	return err
}

func (s *PostgresStore) UpdateTemplate(ctx context.Context, tmpl *models.Template) error {
	variablesJSON, _ := json.Marshal(tmpl.Variables)
	localesJSON, _ := json.Marshal(tmpl.Locales)

	query := `
		UPDATE templates
		SET name = $2, channel = $3, subject = $4, body = $5, variables = $6, locales = $7,
		    version = $8, is_active = $9, updated_at = $10
		WHERE id = $1 AND tenant_id = $11
	`

	result, err := s.db.Exec(ctx, query,
		tmpl.ID, tmpl.Name, tmpl.Channel, tmpl.Subject, tmpl.Body,
		variablesJSON, localesJSON, tmpl.Version, tmpl.IsActive,
		tmpl.UpdatedAt, tmpl.TenantID,
	)

	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		return models.ErrTemplateNotFound
	}

	return nil
}

func (s *PostgresStore) DeleteTemplate(ctx context.Context, tenantID, templateID string) error {
	query := `DELETE FROM templates WHERE id = $1 AND tenant_id = $2`
	result, err := s.db.Exec(ctx, query, templateID, tenantID)
	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		return models.ErrTemplateNotFound
	}

	return nil
}

func (s *PostgresStore) ListTemplates(ctx context.Context, tenantID string, channel models.Channel) ([]*models.Template, error) {
	query := `
		SELECT id, tenant_id, name, channel, subject, body, variables, locales, version, is_active, created_at, updated_at
		FROM templates
		WHERE tenant_id = $1 AND ($2 = '' OR channel = $2) AND is_active = true
		ORDER BY name, version DESC
	`

	rows, err := s.db.Query(ctx, query, tenantID, string(channel))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var templates []*models.Template
	for rows.Next() {
		var tmpl models.Template
		var localesJSON []byte
		var variablesJSON []byte

		err := rows.Scan(
			&tmpl.ID, &tmpl.TenantID, &tmpl.Name, &tmpl.Channel,
			&tmpl.Subject, &tmpl.Body, &variablesJSON, &localesJSON,
			&tmpl.Version, &tmpl.IsActive, &tmpl.CreatedAt, &tmpl.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}

		json.Unmarshal(variablesJSON, &tmpl.Variables)
		json.Unmarshal(localesJSON, &tmpl.Locales)

		templates = append(templates, &tmpl)
	}

	return templates, rows.Err()
}