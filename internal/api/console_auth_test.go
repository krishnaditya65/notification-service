package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func tokenProtectedRouter(token string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	server := &Server{apiToken: token}
	router := gin.New()
	router.GET("/open", func(c *gin.Context) { c.Status(http.StatusOK) })
	router.GET("/api/x", server.requireToken(), func(c *gin.Context) { c.Status(http.StatusOK) })

	return router
}

func statusOf(router *gin.Engine, path, bearer string) int {
	request := httptest.NewRequest(http.MethodGet, path, nil)
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	return recorder.Code
}

func TestTokenGuardsApiOnlyWhenConfigured(t *testing.T) {
	guarded := tokenProtectedRouter("s3cret")

	if statusOf(guarded, "/api/x", "") != http.StatusUnauthorized || statusOf(guarded, "/api/x", "nope") != http.StatusUnauthorized {
		t.Fatal("missing or wrong token must be rejected")
	}

	if statusOf(guarded, "/api/x", "s3cret") != http.StatusOK || statusOf(guarded, "/open", "") != http.StatusOK {
		t.Fatal("valid token and open paths must pass")
	}

	if statusOf(tokenProtectedRouter(""), "/api/x", "") != http.StatusOK {
		t.Fatal("no token configured means open")
	}
}

func TestTenantComesFromQueryOrHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)

	for _, request := range []*http.Request{httptest.NewRequest("GET", "/?tenant_id=acme", nil), func() *http.Request {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("X-Tenant-ID", "acme")
		return r
	}()} {
		context, _ := gin.CreateTestContext(httptest.NewRecorder())
		context.Request = request

		if tenantOf(context) != "acme" {
			t.Fatal("tenant not resolved")
		}
	}
}
