package orchestration

import (
	"errors"
	"testing"

	"github.com/notification-service/naas/pkg/models"
)

func TestPrepareAssignsIdPayloadAndDefaults(t *testing.T) {
	notification := &models.Notification{}
	prepareNotification(notification)

	if notification.ID == "" || notification.Payload == nil || notification.MaxRetries != defaultMaxRetries || notification.Status != models.StatusPending {
		t.Fatalf("defaults missing: %+v", notification)
	}

	existing := &models.Notification{ID: "keep"}
	prepareNotification(existing)

	if existing.ID != "keep" {
		t.Fatal("existing id must be preserved")
	}
}

func TestStatusForFailureSeparatesSuppressionFromFailure(t *testing.T) {
	if statusForFailure(errors.New("user opted out of email")) != models.StatusSuppressed {
		t.Fatal("opt-out should be suppressed")
	}

	if statusForFailure(errors.New("provider send failed")) != models.StatusFailed {
		t.Fatal("provider errors should be failed")
	}
}
