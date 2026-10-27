package reliability

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

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}