package channels

import (
	"sync"
	"time"
)

type TokenBucket struct {
	capacity   int
	tokens     float64
	ratePerSec float64
	lastRefill time.Time
	mu         sync.Mutex
}

func NewTokenBucket(ratePerSec float64, burst int) *TokenBucket {
	return &TokenBucket{
		capacity:   burst,
		tokens:     float64(burst),
		ratePerSec: ratePerSec,
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) Allow(n int) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens = min(float64(tb.capacity), tb.tokens+elapsed*tb.ratePerSec)
	tb.lastRefill = now

	if tb.tokens >= float64(n) {
		tb.tokens -= float64(n)
		return true
	}
	return false
}

func (tb *TokenBucket) Wait(n int) time.Duration {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens = min(float64(tb.capacity), tb.tokens+elapsed*tb.ratePerSec)
	tb.lastRefill = now

	if tb.tokens >= float64(n) {
		tb.tokens -= float64(n)
		return 0
	}

	needed := float64(n) - tb.tokens
	waitTime := needed / tb.ratePerSec
	return time.Duration(waitTime * float64(time.Second))
}

type CircuitBreaker struct {
	failureThreshold int
	successThreshold int
	timeout          time.Duration
	state            string
	failureCount     int
	successCount     int
	lastFailure      *time.Time
	mu               sync.Mutex
}

func NewCircuitBreaker(failureThreshold, successThreshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		timeout:          timeout,
		state:            "closed",
	}
}

func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case "closed":
		return true
	case "open":
		if cb.lastFailure != nil && time.Since(*cb.lastFailure) > cb.timeout {
			cb.state = "half-open"
			cb.successCount = 0
			return true
		}
		return false
	case "half-open":
		return true
	default:
		return true
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case "half-open":
		cb.successCount++
		if cb.successCount >= cb.successThreshold {
			cb.state = "closed"
			cb.failureCount = 0
		}
	case "closed":
		cb.failureCount = 0
	}
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	cb.lastFailure = &now
	cb.failureCount++

	switch cb.state {
	case "closed":
		if cb.failureCount >= cb.failureThreshold {
			cb.state = "open"
		}
	case "half-open":
		cb.state = "open"
		cb.successCount = 0
	}
}

func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}