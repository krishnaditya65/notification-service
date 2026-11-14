package api

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/notification-service/naas/internal/notificationstore"
	"github.com/notification-service/naas/internal/orchestration"
	"github.com/notification-service/naas/internal/preferences"
	"github.com/notification-service/naas/internal/processing"
	"github.com/notification-service/naas/internal/reliability"
	"github.com/notification-service/naas/internal/templates"
	"github.com/notification-service/naas/pkg/models"
	"go.uber.org/zap"
)

type Server struct {
	config           *Config
	orchestrator     *orchestration.Orchestrator
	processor        *processing.Processor
	reliabilityMgr   *reliability.Manager
	preferenceMgr    *preferences.Manager
	templateEngine   *templates.Engine
	logger           *zap.Logger
	httpServer       *http.Server
	notifications    *notificationstore.Store
	apiToken         string
}

type Config struct {
	Port         string
	Host         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

func NewServer(
	cfg *Config,
	orchestrator *orchestration.Orchestrator,
	processor *processing.Processor,
	reliabilityMgr *reliability.Manager,
	preferenceMgr *preferences.Manager,
	templateEngine *templates.Engine,
	logger *zap.Logger,
) *Server {
	return &Server{
		config:         cfg,
		orchestrator:   orchestrator,
		processor:      processor,
		reliabilityMgr: reliabilityMgr,
		preferenceMgr:  preferenceMgr,
		templateEngine: templateEngine,
		logger:         logger,
	}
}

func (s *Server) Start() error {
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(s.loggingMiddleware())
	router.Use(s.errorMiddleware())

	s.setupRoutes(router)

	s.httpServer = &http.Server{
		Addr:         s.config.Host + ":" + s.config.Port,
		Handler:      router,
		ReadTimeout:  s.config.ReadTimeout,
		WriteTimeout: s.config.WriteTimeout,
		IdleTimeout:  s.config.IdleTimeout,
	}

	s.logger.Info("Starting HTTP server", zap.String("addr", s.httpServer.Addr))
	return s.httpServer.ListenAndServe()
}

func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Stopping HTTP server")
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) setupRoutes(router *gin.Engine) {
	router.GET("/health", s.healthCheck)
	router.GET("/ready", s.readinessCheck)

	s.mountConsole(router)

	v1 := router.Group("/api/v1")
	v1.Use(s.requireToken())
	{
		v1.GET("/stats/overview", s.getOverview)
		v1.GET("/tenants", s.listTenants)
		v1.GET("/config/summary", s.getConfigSummary)
		v1.POST("/notifications", s.sendNotification)
		v1.POST("/notifications/bulk", s.sendBulkNotification)
		v1.GET("/notifications/:id", s.getNotification)
		v1.GET("/notifications", s.listNotifications)
		v1.POST("/notifications/:id/retry", s.retryNotification)

		v1.POST("/templates", s.createTemplate)
		v1.GET("/templates/:id", s.getTemplate)
		v1.PUT("/templates/:id", s.updateTemplate)
		v1.DELETE("/templates/:id", s.deleteTemplate)
		v1.GET("/templates", s.listTemplates)
		v1.POST("/templates/render", s.renderTemplate)

		v1.GET("/preferences/:user_id", s.getPreferences)
		v1.PUT("/preferences/:user_id", s.updatePreferences)
		v1.POST("/preferences/:user_id/opt-out", s.optOut)
		v1.POST("/preferences/:user_id/opt-in", s.optIn)
		v1.POST("/preferences/:user_id/quiet-hours", s.setQuietHours)
		v1.POST("/preferences/:user_id/frequency-cap", s.setFrequencyCap)

		v1.GET("/dlq", s.listDLQ)
		v1.POST("/dlq/:id/retry", s.retryDLQ)

		v1.GET("/digest/rules", s.listDigestRules)
		v1.POST("/digest/rules", s.createDigestRule)
		v1.GET("/digest/rules/:id", s.getDigestRule)

		v1.GET("/stats/rate-limits", s.getRateLimitStats)
		v1.GET("/stats/providers", s.getProviderStats)
	}
}

func (s *Server) healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "healthy"})
}

func (s *Server) readinessCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

func (s *Server) sendNotification(c *gin.Context) {
	var req SendNotificationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	notification := &models.Notification{
		TenantID:    req.TenantID,
		UserID:      req.UserID,
		TemplateID:  req.TemplateID,
		Channel:     models.Channel(req.Channel),
		Priority:    models.Priority(req.Priority),
		Variables:   req.Variables,
		Metadata:    req.Metadata,
		Locale:      req.Locale,
		IdempotencyKey: req.IdempotencyKey,
	}

	if notification.Priority == "" {
		notification.Priority = models.PriorityNormal
	}
	if notification.Locale == "" {
		notification.Locale = "en"
	}

	if req.ScheduledAt != nil {
		notification.ScheduledAt = req.ScheduledAt
	}

	if err := s.orchestrator.Execute(c.Request.Context(), notification, s.processor); err != nil {
		s.logger.Error("Failed to send notification", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"id":       notification.ID,
		"status":   notification.Status,
		"message":  "Notification queued for delivery",
	})
}

func (s *Server) sendBulkNotification(c *gin.Context) {
	var req BulkNotificationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	job := &processing.BulkJob{
		TenantID:   req.TenantID,
		TemplateID: req.TemplateID,
		Channel:    models.Channel(req.Channel),
		UserIDs:    req.UserIDs,
		Variables:  req.Variables,
		Priority:   models.Priority(req.Priority),
	}

	if job.Priority == "" {
		job.Priority = models.PriorityNormal
	}

	if err := s.orchestrator.ExecuteBulk(c.Request.Context(), job, s.processor); err != nil {
		s.logger.Error("Failed to start bulk job", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"job_id":  job.ID,
		"status":  job.Status,
		"total":   job.Total,
		"message": "Bulk notification job started",
	})
}

func (s *Server) createTemplate(c *gin.Context) {
	var req CreateTemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	tmpl := &models.Template{
		ID:        models.NewID("tpl"),
		TenantID:  req.TenantID,
		Name:      req.Name,
		Channel:   models.Channel(req.Channel),
		Subject:   req.Subject,
		Body:      req.Body,
		Variables: req.Variables,
		Locales:   req.Locales,
		IsActive:  true,
	}

	if err := s.templateEngine.CreateTemplate(c.Request.Context(), tmpl); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, tmpl)
}

func (s *Server) getTemplate(c *gin.Context) {
	tmpl, err := s.templateEngine.GetTemplate(c.Request.Context(), tenantOf(c), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, tmpl)
}

func (s *Server) renderTemplate(c *gin.Context) {
	var req RenderTemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	subject, body, err := s.templateEngine.RenderByName(c.Request.Context(), req.TenantID, req.Name, models.Channel(req.Channel), req.Locale, req.Variables)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"subject": subject, "body": body})
}

func (s *Server) getPreferences(c *gin.Context) {
	prefs, err := s.preferenceMgr.GetPreferences(c.Request.Context(), tenantOf(c), c.Param("user_id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, prefs)
}

func (s *Server) updatePreferences(c *gin.Context) {
	var prefs models.UserPreferences
	if err := c.ShouldBindJSON(&prefs); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	prefs.TenantID = tenantOf(c)
	prefs.UserID = c.Param("user_id")

	if err := s.preferenceMgr.UpsertPreferences(c.Request.Context(), &prefs); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, prefs)
}

func (s *Server) optOut(c *gin.Context) {
	channel := models.Channel(c.Query("channel"))
	if err := s.preferenceMgr.OptOut(c.Request.Context(), tenantOf(c), c.Param("user_id"), channel); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "opted out"})
}

func (s *Server) optIn(c *gin.Context) {
	channel := models.Channel(c.Query("channel"))
	if err := s.preferenceMgr.OptIn(c.Request.Context(), tenantOf(c), c.Param("user_id"), channel); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "opted in"})
}

func (s *Server) setQuietHours(c *gin.Context) {
	var req QuietHoursRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := s.preferenceMgr.SetQuietHours(c.Request.Context(), tenantOf(c), c.Param("user_id"), req.Enabled, req.StartHour, req.EndHour, req.Timezone); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "quiet hours updated"})
}

func (s *Server) setFrequencyCap(c *gin.Context) {
	var req FrequencyCapRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := s.preferenceMgr.SetFrequencyCap(c.Request.Context(), tenantOf(c), c.Param("user_id"), req.Category, req.MaxCount, req.Window); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "frequency cap set"})
}

func (s *Server) listDLQ(c *gin.Context) {
	entries, err := s.reliabilityMgr.GetDLQEntries(c.Request.Context(), 100, 0)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, entries)
}

func (s *Server) retryDLQ(c *gin.Context) {
	if err := s.reliabilityMgr.RetryDLQEntry(c.Request.Context(), c.Param("id"), s.orchestrator); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "retry queued"})
}

func (s *Server) listDigestRules(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}

func (s *Server) createDigestRule(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}

func (s *Server) getDigestRule(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}

func (s *Server) getRateLimitStats(c *gin.Context) {
	stats := s.reliabilityMgr.GetRateLimitStatus()
	c.JSON(http.StatusOK, stats)
}

func (s *Server) loggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		s.logger.Info("HTTP request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", time.Since(start)),
		)
	}
}

func (s *Server) errorMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		if len(c.Errors) > 0 {
			s.logger.Error("Request error", zap.String("path", c.Request.URL.Path), zap.Strings("errors", c.Errors.Errors()))
		}
	}
}

type SendNotificationRequest struct {
	TenantID       string                 `json:"tenant_id" binding:"required"`
	UserID         string                 `json:"user_id" binding:"required"`
	TemplateID     string                 `json:"template_id"`
	Channel        string                 `json:"channel" binding:"required"`
	Priority       string                 `json:"priority"`
	Variables      map[string]interface{} `json:"variables"`
	Metadata       map[string]interface{} `json:"metadata"`
	Locale         string                 `json:"locale"`
	IdempotencyKey string                 `json:"idempotency_key"`
	ScheduledAt    *time.Time             `json:"scheduled_at"`
}

type BulkNotificationRequest struct {
	TenantID   string                 `json:"tenant_id" binding:"required"`
	TemplateID string                 `json:"template_id" binding:"required"`
	Channel    string                 `json:"channel" binding:"required"`
	UserIDs    []string               `json:"user_ids" binding:"required"`
	Variables  map[string]interface{} `json:"variables"`
	Priority   string                 `json:"priority"`
}

type CreateTemplateRequest struct {
	TenantID  string                 `json:"tenant_id" binding:"required"`
	Name      string                 `json:"name" binding:"required"`
	Channel   string                 `json:"channel" binding:"required"`
	Subject   string                 `json:"subject"`
	Body      string                 `json:"body" binding:"required"`
	Variables []string               `json:"variables"`
	Locales   map[string]models.TemplateLocale `json:"locales"`
}

type RenderTemplateRequest struct {
	TenantID string                 `json:"tenant_id" binding:"required"`
	Name     string                 `json:"name" binding:"required"`
	Channel  string                 `json:"channel" binding:"required"`
	Locale   string                 `json:"locale"`
	Variables map[string]interface{} `json:"variables"`
}

type QuietHoursRequest struct {
	Enabled   bool   `json:"enabled"`
	StartHour int    `json:"start_hour"`
	EndHour   int    `json:"end_hour"`
	Timezone  string `json:"timezone"`
}

type FrequencyCapRequest struct {
	Category string        `json:"category"`
	MaxCount int           `json:"max_count"`
	Window   time.Duration `json:"window"`
}