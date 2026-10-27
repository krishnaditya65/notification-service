package errors

import (
	"errors"
	"fmt"
)

var (
	ErrNotificationNotFound    = errors.New("notification not found")
	ErrTemplateNotFound        = errors.New("template not found")
	ErrUserPreferencesNotFound = errors.New("user preferences not found")
	ErrChannelNotSupported     = errors.New("channel not supported")
	ErrProviderNotAvailable    = errors.New("provider not available")
	ErrRateLimitExceeded       = errors.New("rate limit exceeded")
	ErrCircuitOpen             = errors.New("circuit breaker open")
	ErrInvalidPayload          = errors.New("invalid payload")
	ErrIdempotencyConflict     = errors.New("idempotency conflict")
	ErrQuietHoursActive        = errors.New("quiet hours active")
	ErrFrequencyCapExceeded    = errors.New("frequency cap exceeded")
	ErrOptedOut                = errors.New("user opted out")
	ErrTemplateRenderFailed    = errors.New("template render failed")
	ErrProviderError           = errors.New("provider error")
	ErrValidationFailed        = errors.New("validation failed")
	ErrUnauthorized            = errors.New("unauthorized")
	ErrForbidden               = errors.New("forbidden")
	ErrInternalError           = errors.New("internal error")
)

type AppError struct {
	Code    string
	Message string
	Details map[string]interface{}
	Err     error
}

func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *AppError) Unwrap() error {
	return e.Err
}

func NewAppError(code, message string, err error, details map[string]interface{}) *AppError {
	return &AppError{
		Code:    code,
		Message: message,
		Err:     err,
		Details: details,
	}
}

func Wrap(err error, code, message string, details map[string]interface{}) *AppError {
	return &AppError{
		Code:    code,
		Message: message,
		Err:     err,
		Details: details,
	}
}

func Is(err error, target error) bool {
	return errors.Is(err, target)
}

func As(err error, target interface{}) bool {
	return errors.As(err, target)
}

func Register() error {
	return nil
}