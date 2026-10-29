package contracts

import (
	"encoding/json"
	"sitepulse/internal/platform"
	"time"
)

const (
	AccountsTopic         = "sitepulse.accounts.v1"
	IncidentsTopic        = "sitepulse.incidents.v1"
	VerificationRequested = "email.verification_requested"
	IncidentOpened        = "incident.opened"
	IncidentResolved      = "incident.resolved"
)

type Event struct {
	ID         string    `json:"event_id"`
	Type       string    `json:"event_type"`
	Version    int       `json:"version"`
	OccurredAt time.Time `json:"occurred_at"`
	Data       Data      `json:"data"`
}
type Data struct {
	UserID          string     `json:"user_id,omitempty"`
	MonitorID       string     `json:"monitor_id,omitempty"`
	IncidentID      string     `json:"incident_id,omitempty"`
	MonitorName     string     `json:"monitor_name,omitempty"`
	URL             string     `json:"url,omitempty"`
	Reason          string     `json:"reason,omitempty"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	ResolvedAt      *time.Time `json:"resolved_at,omitempty"`
	DurationSeconds int64      `json:"duration_seconds,omitempty"`
	RecipientEmail  *string    `json:"recipient_email"`
	VerificationURL string     `json:"verification_url,omitempty"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
}

func New(kind string, data Data) Event {
	return Event{ID: platform.ID(), Type: kind, Version: 1, OccurredAt: time.Now().UTC(), Data: data}
}
func (e Event) JSON() ([]byte, error) { return json.Marshal(e) }
