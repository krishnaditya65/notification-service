package templates

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/notification-service/naas/pkg/errors"
	"github.com/notification-service/naas/pkg/models"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"gopkg.in/yaml.v3"
)

type Engine struct {
	store       *PostgresStore
	redis       *redis.Client
	logger      *zap.Logger
	cache       map[string]*models.Template
	cacheMutex  sync.RWMutex
	cacheTTL    time.Duration
	printers    map[string]*message.Printer
	printersMu  sync.RWMutex
}

type StoreInterface interface {
	GetTemplate(ctx context.Context, tenantID, templateID string) (*models.Template, error)
	GetTemplateByName(ctx context.Context, tenantID, name, channel string) (*models.Template, error)
	CreateTemplate(ctx context.Context, template *models.Template) error
	UpdateTemplate(ctx context.Context, template *models.Template) error
	DeleteTemplate(ctx context.Context, tenantID, templateID string) error
	ListTemplates(ctx context.Context, tenantID string, channel models.Channel) ([]*models.Template, error)
}

func NewEngine(store *PostgresStore, redisClient *redis.Client, logger *zap.Logger) *Engine {
	return &Engine{
		store:      store,
		redis:      redisClient,
		logger:     logger,
		cache:      make(map[string]*models.Template),
		cacheTTL:   5 * time.Minute,
		printers:   make(map[string]*message.Printer),
	}
}

func (e *Engine) GetTemplate(ctx context.Context, tenantID, templateID string) (*models.Template, error) {
	cacheKey := fmt.Sprintf("template:%s:%s", tenantID, templateID)

	e.cacheMutex.RLock()
	if cached, ok := e.cache[cacheKey]; ok {
		e.cacheMutex.RUnlock()
		return cached, nil
	}
	e.cacheMutex.RUnlock()

	cachedData, err := e.redis.Get(ctx, cacheKey).Bytes()
	if err == nil {
		var tmpl models.Template
		if json.Unmarshal(cachedData, &tmpl) == nil {
			e.cacheMutex.Lock()
			e.cache[cacheKey] = &tmpl
			e.cacheMutex.Unlock()
			return &tmpl, nil
		}
	}

	tmpl, err := e.store.GetTemplate(ctx, tenantID, templateID)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrTemplateNotFound.Error(), "failed to get template", map[string]interface{}{
			"tenant_id": tenantID,
			"template_id": templateID,
		})
	}

	e.cacheTemplate(cacheKey, tmpl)
	return tmpl, nil
}

func (e *Engine) GetTemplateByName(ctx context.Context, tenantID, name string, channel models.Channel) (*models.Template, error) {
	cacheKey := fmt.Sprintf("template:name:%s:%s:%s", tenantID, name, channel)

	e.cacheMutex.RLock()
	if cached, ok := e.cache[cacheKey]; ok {
		e.cacheMutex.RUnlock()
		return cached, nil
	}
	e.cacheMutex.RUnlock()

	tmpl, err := e.store.GetTemplateByName(ctx, tenantID, name, string(channel))
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrTemplateNotFound.Error(), "failed to get template by name", map[string]interface{}{
			"tenant_id": tenantID,
			"name": name,
			"channel": channel,
		})
	}

	e.cacheTemplate(cacheKey, tmpl)
	return tmpl, nil
}

func (e *Engine) cacheTemplate(key string, tmpl *models.Template) {
	data, _ := json.Marshal(tmpl)
	e.redis.Set(context.Background(), key, data, e.cacheTTL)

	e.cacheMutex.Lock()
	e.cache[key] = tmpl
	e.cacheMutex.Unlock()
}

func (e *Engine) Render(ctx context.Context, tenantID, templateID, locale string, variables map[string]interface{}) (subject, body string, err error) {
	tmpl, err := e.GetTemplate(ctx, tenantID, templateID)
	if err != nil {
		return "", "", err
	}

	return e.renderTemplate(tmpl, locale, variables)
}

func (e *Engine) RenderByName(ctx context.Context, tenantID, name string, channel models.Channel, locale string, variables map[string]interface{}) (subject, body string, err error) {
	tmpl, err := e.GetTemplateByName(ctx, tenantID, name, channel)
	if err != nil {
		return "", "", err
	}

	return e.renderTemplate(tmpl, locale, variables)
}

func (e *Engine) renderTemplate(tmpl *models.Template, locale string, variables map[string]interface{}) (string, string, error) {
	effectiveLocale := locale
	if effectiveLocale == "" {
		effectiveLocale = "en"
	}

	subject := tmpl.Subject
	body := tmpl.Body

	if loc, ok := tmpl.Locales[effectiveLocale]; ok {
		if loc.Subject != "" {
			subject = loc.Subject
		}
		if loc.Body != "" {
			body = loc.Body
		}
	}

	subject, err := e.interpolate(subject, variables, effectiveLocale)
	if err != nil {
		return "", "", errors.Wrap(err, errors.ErrTemplateRenderFailed.Error(), "failed to render subject", nil)
	}

	body, err = e.interpolate(body, variables, effectiveLocale)
	if err != nil {
		return "", "", errors.Wrap(err, errors.ErrTemplateRenderFailed.Error(), "failed to render body", nil)
	}

	return subject, body, nil
}

func (e *Engine) interpolate(template string, variables map[string]interface{}, locale string) (string, error) {
	result := template

	for key, value := range variables {
		placeholder := fmt.Sprintf("{{%s}}", key)
		strValue := fmt.Sprintf("%v", value)
		result = strings.ReplaceAll(result, placeholder, strValue)
	}

	if strings.Contains(result, "{{") && strings.Contains(result, "}}") {
		printer := e.getPrinter(locale)
		result = printer.Sprintf(result)
	}

	return result, nil
}

func (e *Engine) getPrinter(locale string) *message.Printer {
	e.printersMu.RLock()
	if p, ok := e.printers[locale]; ok {
		e.printersMu.RUnlock()
		return p
	}
	e.printersMu.RUnlock()

	tag, err := language.Parse(locale)
	if err != nil {
		tag = language.English
	}

	printer := message.NewPrinter(tag)

	e.printersMu.Lock()
	e.printers[locale] = printer
	e.printersMu.Unlock()

	return printer
}

func (e *Engine) CreateTemplate(ctx context.Context, tmpl *models.Template) error {
	tmpl.CreatedAt = time.Now()
	tmpl.UpdatedAt = time.Now()
	tmpl.Version = 1

	if err := e.validateTemplate(tmpl); err != nil {
		return err
	}

	if err := e.store.CreateTemplate(ctx, tmpl); err != nil {
		return errors.Wrap(err, errors.ErrInternalError.Error(), "failed to create template", nil)
	}

	e.invalidateCache(tmpl.TenantID, tmpl.ID)
	return nil
}

func (e *Engine) UpdateTemplate(ctx context.Context, tmpl *models.Template) error {
	tmpl.UpdatedAt = time.Now()
	tmpl.Version++

	if err := e.validateTemplate(tmpl); err != nil {
		return err
	}

	if err := e.store.UpdateTemplate(ctx, tmpl); err != nil {
		return errors.Wrap(err, errors.ErrInternalError.Error(), "failed to update template", nil)
	}

	e.invalidateCache(tmpl.TenantID, tmpl.ID)
	return nil
}

func (e *Engine) DeleteTemplate(ctx context.Context, tenantID, templateID string) error {
	if err := e.store.DeleteTemplate(ctx, tenantID, templateID); err != nil {
		return errors.Wrap(err, errors.ErrInternalError.Error(), "failed to delete template", nil)
	}

	e.invalidateCache(tenantID, templateID)
	return nil
}

func (e *Engine) ListTemplates(ctx context.Context, tenantID string, channel models.Channel) ([]*models.Template, error) {
	return e.store.ListTemplates(ctx, tenantID, channel)
}

func (e *Engine) validateTemplate(tmpl *models.Template) error {
	if tmpl.TenantID == "" {
		return errors.NewAppError("VALIDATION_ERROR", "tenant_id is required", nil, nil)
	}
	if tmpl.Name == "" {
		return errors.NewAppError("VALIDATION_ERROR", "name is required", nil, nil)
	}
	if tmpl.Channel == "" {
		return errors.NewAppError("VALIDATION_ERROR", "channel is required", nil, nil)
	}
	if tmpl.Body == "" {
		return errors.NewAppError("VALIDATION_ERROR", "body is required", nil, nil)
	}
	return nil
}

func (e *Engine) invalidateCache(tenantID, templateID string) {
	keys := []string{
		fmt.Sprintf("template:%s:%s", tenantID, templateID),
	}

	e.cacheMutex.Lock()
	for _, key := range keys {
		delete(e.cache, key)
	}
	e.cacheMutex.Unlock()

	e.redis.Del(context.Background(), keys...)
}

func (e *Engine) ParseTemplateSpec(spec string) (*models.Template, error) {
	var tmpl models.Template

	if strings.HasPrefix(strings.TrimSpace(spec), "{") {
		if err := json.Unmarshal([]byte(spec), &tmpl); err != nil {
			return nil, err
		}
	} else {
		if err := yaml.Unmarshal([]byte(spec), &tmpl); err != nil {
			return nil, err
		}
	}

	return &tmpl, nil
}

func (e *Engine) ValidateVariables(templateID string, variables map[string]interface{}) ([]string, error) {
	tmpl, err := e.store.GetTemplate(context.Background(), "", templateID)
	if err != nil {
		return nil, err
	}

	var missing []string
	for _, required := range tmpl.Variables {
		if _, ok := variables[required]; !ok {
			missing = append(missing, required)
		}
	}

	return missing, nil
}

type TemplateRenderer interface {
	Render(subject, body string, variables map[string]interface{}) (string, string, error)
}

func (e *Engine) RenderWithLayout(ctx context.Context, tenantID, templateID, layoutID, locale string, variables map[string]interface{}) (subject, body string, err error) {
	tmpl, err := e.GetTemplate(ctx, tenantID, templateID)
	if err != nil {
		return "", "", err
	}

	var layout *models.Template
	if layoutID != "" {
		layout, err = e.GetTemplate(ctx, tenantID, layoutID)
		if err != nil {
			return "", "", err
		}
	}

	subject, body, err = e.renderTemplate(tmpl, locale, variables)
	if err != nil {
		return "", "", err
	}

	if layout != nil {
		layoutVars := map[string]interface{}{
			"content": body,
			"title":   subject,
		}
		for k, v := range variables {
			layoutVars[k] = v
		}
		_, body, err = e.renderTemplate(layout, locale, layoutVars)
		if err != nil {
			return "", "", err
		}
	}

	return subject, body, nil
}