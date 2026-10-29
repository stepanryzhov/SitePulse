package monitor

import (
	"errors"
	"time"
)

var (
	ErrNotFound    = errors.New("resource not found")
	ErrConflict    = errors.New("email already registered")
	ErrLimit       = errors.New("monitor limit reached (20)")
	ErrCredentials = errors.New("invalid email or password")
	ErrRateLimit   = errors.New("please wait before requesting another verification email")
	ErrToken       = errors.New("verification link is invalid or expired")
)

type User struct {
	ID           string     `json:"id"`
	Email        string     `json:"email"`
	PasswordHash string     `json:"-"`
	VerifiedAt   *time.Time `json:"email_verified_at"`
}
type Monitor struct {
	ID             string     `json:"id"`
	UserID         string     `json:"-"`
	Name           string     `json:"name"`
	URL            string     `json:"url"`
	Interval       int        `json:"interval_seconds"`
	Timeout        int        `json:"timeout_seconds"`
	ExpectedStatus int        `json:"expected_status"`
	Status         string     `json:"status"`
	Paused         bool       `json:"is_paused"`
	NextCheckAt    time.Time  `json:"next_check_at"`
	LastCheckedAt  *time.Time `json:"last_checked_at"`
	LastResponseMS *int64     `json:"last_response_ms"`
	Failures       int        `json:"consecutive_failures"`
	Successes      int        `json:"consecutive_successes"`
	LeaseToken     *string    `json:"-"`
	CreatedAt      time.Time  `json:"created_at"`
}
type MonitorInput struct {
	Name           string `json:"name"`
	URL            string `json:"url"`
	Interval       int    `json:"interval_seconds"`
	Timeout        int    `json:"timeout_seconds"`
	ExpectedStatus int    `json:"expected_status"`
}
type Check struct {
	ID         string    `json:"id"`
	CheckedAt  time.Time `json:"checked_at"`
	ResponseMS int64     `json:"response_time_ms"`
	StatusCode *int      `json:"status_code"`
	Success    bool      `json:"success"`
	ErrorType  *string   `json:"error_type"`
}
type Incident struct {
	ID              string     `json:"id"`
	StartedAt       time.Time  `json:"started_at"`
	ResolvedAt      *time.Time `json:"resolved_at"`
	Cause           string     `json:"cause"`
	DurationSeconds int64      `json:"duration_seconds"`
}
type Stats struct {
	Period       string   `json:"period"`
	Total        int64    `json:"total_checks"`
	Successful   int64    `json:"successful_checks"`
	Availability *float64 `json:"availability_percent"`
	AverageMS    *float64 `json:"average_response_ms"`
}
