package api

import (
	"crypto/subtle"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/notification-service/naas/internal/notificationstore"
	"github.com/notification-service/naas/pkg/models"
	"github.com/notification-service/naas/ui"
)

// SetConsole wires the notification store (list/stats queries) and the optional bearer token for /api/v1.
func (s *Server) SetConsole(store *notificationstore.Store, apiToken string) {
	s.notifications, s.apiToken = store, apiToken
}

// tenantOf reads the tenant from ?tenant_id= or the X-Tenant-ID header.
func tenantOf(c *gin.Context) string {
	if tenant := c.Query("tenant_id"); tenant != "" {
		return tenant
	}

	return c.GetHeader("X-Tenant-ID")
}

func (s *Server) mountConsole(router *gin.Engine) {
	router.StaticFS("/ui", http.FS(ui.Files))
	router.GET("/", func(c *gin.Context) { c.Redirect(http.StatusFound, "/ui/") })
}

func (s *Server) requireToken() gin.HandlerFunc {
	return func(c *gin.Context) {
		if s.apiToken == "" {
			c.Next()
			return
		}

		presented := strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(presented), []byte(s.apiToken)) != 1 {
			c.Header("WWW-Authenticate", `Bearer realm="naas"`)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}

		c.Next()
	}
}

func (s *Server) storeUnavailable(c *gin.Context) bool {
	if s.notifications == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "notification store not configured"})
		return true
	}

	return false
}

func (s *Server) listNotifications(c *gin.Context) {
	if s.storeUnavailable(c) {
		return
	}

	limit, _ := strconv.Atoi(c.Query("limit"))
	offset, _ := strconv.Atoi(c.Query("offset"))

	result, err := s.notifications.List(c.Request.Context(), notificationstore.Filter{
		TenantID: tenantOf(c), Status: c.Query("status"), Channel: c.Query("channel"), Provider: c.Query("provider"),
		UserID: c.Query("user_id"), Text: c.Query("q"), Limit: limit, Offset: offset,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (s *Server) getNotification(c *gin.Context) {
	if s.storeUnavailable(c) {
		return
	}

	notification, err := s.notifications.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "notification not found"})
		return
	}

	c.JSON(http.StatusOK, notification)
}

// retryNotification re-runs a failed notification through the normal pipeline.
func (s *Server) retryNotification(c *gin.Context) {
	if s.storeUnavailable(c) {
		return
	}

	notification, err := s.notifications.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "notification not found"})
		return
	}

	if notification.Status != models.StatusFailed && notification.Status != models.StatusBounced {
		c.JSON(http.StatusConflict, gin.H{"error": "only failed or bounced notifications can be retried"})
		return
	}

	notification.Status, notification.Error, notification.RetryCount = models.StatusPending, "", notification.RetryCount+1

	if err := s.orchestrator.Execute(c.Request.Context(), notification, s.processor); err != nil {
		c.JSON(http.StatusOK, gin.H{"id": notification.ID, "status": notification.Status, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"id": notification.ID, "status": notification.Status})
}

func (s *Server) listTemplates(c *gin.Context) {
	templates, err := s.templateEngine.ListTemplates(c.Request.Context(), tenantOf(c), models.Channel(c.Query("channel")))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if templates == nil {
		templates = []*models.Template{}
	}

	c.JSON(http.StatusOK, templates)
}

func (s *Server) updateTemplate(c *gin.Context) {
	var req CreateTemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	existing, err := s.templateEngine.GetTemplate(c.Request.Context(), req.TenantID, c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "template not found"})
		return
	}

	existing.Name, existing.Channel, existing.Subject, existing.Body = req.Name, models.Channel(req.Channel), req.Subject, req.Body
	existing.Variables, existing.Locales = req.Variables, req.Locales

	if err := s.templateEngine.UpdateTemplate(c.Request.Context(), existing); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, existing)
}

func (s *Server) deleteTemplate(c *gin.Context) {
	if err := s.templateEngine.DeleteTemplate(c.Request.Context(), tenantOf(c), c.Param("id")); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "template deleted"})
}

const defaultOverviewHours = 24

func (s *Server) getOverview(c *gin.Context) {
	if s.storeUnavailable(c) {
		return
	}

	hours, err := strconv.Atoi(c.Query("hours"))
	if err != nil || hours <= 0 {
		hours = defaultOverviewHours
	}

	overview, err := s.notifications.Overview(c.Request.Context(), tenantOf(c), time.Now().Add(-time.Duration(hours)*time.Hour))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, overview)
}

func (s *Server) getProviderStats(c *gin.Context) {
	if s.storeUnavailable(c) {
		return
	}

	usage, err := s.notifications.ProviderBreakdown(c.Request.Context(), time.Now().Add(-defaultOverviewHours*time.Hour))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"health": s.orchestrator.ProviderHealth(c.Request.Context()), "usage_24h": usage})
}

func (s *Server) listTenants(c *gin.Context) {
	if s.storeUnavailable(c) {
		return
	}

	tenants, err := s.notifications.Tenants(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, tenants)
}

func (s *Server) getConfigSummary(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"rate_limits": s.reliabilityMgr.GetRateLimitStatus(), "auth_enabled": s.apiToken != "", "store_enabled": s.notifications != nil})
}
