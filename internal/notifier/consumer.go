package notifier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/mail"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"sitepulse/internal/contracts"
	"sitepulse/internal/platform"
)

type Service struct {
	DB        *pgxpool.Pool
	PublicURL string
	Mailer    *Mailer
	Log       *slog.Logger
}
type notification struct {
	ID, EventID, Recipient, Subject, Body, Lease string
	Attempts                                     int
}

func (s *Service) Consume(ctx context.Context, client *kgo.Client) {
	defer client.AllowRebalance()
	for ctx.Err() == nil {
		fetches := client.PollRecords(ctx, 1)
		if fetches.IsClientClosed() {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			if ctx.Err() != nil {
				return
			}
			s.Log.Warn("Kafka fetch failed; retrying")
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
		records := fetches.Records()
		for _, record := range records {
			// Keep retrying this durable write before reading or committing later records.
			for ctx.Err() == nil {
				err := s.accept(ctx, record)
				if err == nil {
					if err = client.CommitRecords(ctx, record); err != nil && ctx.Err() == nil {
						s.Log.Warn("offset commit failed; duplicate event is safe")
					}
					break
				}
				s.Log.Warn("event persistence failed; offset not committed")
				select {
				case <-ctx.Done():
					return
				case <-time.After(2 * time.Second):
				}
			}
		}
		client.AllowRebalance()
	}
}
func (s *Service) accept(ctx context.Context, record *kgo.Record) error {
	var event contracts.Event
	reason := ""
	if len(record.Value) > 1<<20 {
		reason = "event too large"
	} else if json.Unmarshal(record.Value, &event) != nil {
		reason = "invalid JSON"
	} else {
		reason = validateEvent(event, record.Topic, s.PublicURL)
	}
	if reason != "" {
		var id *string
		if platform.ValidID(event.ID) {
			id = &event.ID
		}
		_, err := s.DB.Exec(ctx, `INSERT INTO rejected_events(event_id,topic,partition,"offset",reason) VALUES($1,$2,$3,$4,$5) ON CONFLICT(topic,partition,"offset") DO NOTHING`, id, record.Topic, record.Partition, record.Offset, reason)
		if err == nil {
			s.Log.Warn("event rejected", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "reason", reason)
		}
		return err
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `INSERT INTO processed_events(event_id,event_type) VALUES($1,$2) ON CONFLICT DO NOTHING`, event.ID, event.Type)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	if event.Data.RecipientEmail != nil {
		subject, body := compose(event, s.PublicURL)
		_, err = tx.Exec(ctx, `INSERT INTO notifications(id,event_id,recipient_email,subject,body) VALUES($1,$2,$3,$4,$5)`, platform.ID(), event.ID, *event.Data.RecipientEmail, subject, body)
		if err != nil {
			return err
		}
	}
	if err = tx.Commit(ctx); err == nil {
		s.Log.Info("event accepted", "event_id", event.ID, "event_type", event.Type)
	}
	return err
}
func validateEvent(e contracts.Event, topic, publicURL string) string {
	if !platform.ValidID(e.ID) || e.Version != 1 || e.OccurredAt.IsZero() {
		return "invalid envelope or unsupported version"
	}
	d := e.Data
	if d.RecipientEmail != nil {
		raw := *d.RecipientEmail
		address, err := mail.ParseAddress(raw)
		if err != nil || address.Address != raw || len(raw) > 254 || strings.ContainsAny(raw, "\r\n") {
			return "invalid recipient"
		}
	}
	switch e.Type {
	case contracts.VerificationRequested:
		if topic != contracts.AccountsTopic || !platform.ValidID(d.UserID) || d.RecipientEmail == nil || d.ExpiresAt == nil || !d.ExpiresAt.After(e.OccurredAt) {
			return "invalid verification event"
		}
		u, err := url.Parse(d.VerificationURL)
		base, _ := url.Parse(publicURL)
		if err != nil || u.Scheme != base.Scheme || u.Host != base.Host || u.Path != "/verify-email" || u.User != nil || u.Fragment != "" || len(u.Query().Get("token")) != 64 {
			return "invalid verification URL"
		}
	case contracts.IncidentOpened, contracts.IncidentResolved:
		if topic != contracts.IncidentsTopic || !platform.ValidID(d.MonitorID) || !platform.ValidID(d.IncidentID) || d.StartedAt == nil || d.StartedAt.IsZero() || len(d.MonitorName) == 0 || len(d.MonitorName) > 120 || strings.ContainsAny(d.MonitorName, "\r\n") {
			return "invalid incident data"
		}
		u, err := url.Parse(d.URL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || len(d.URL) > 2048 || strings.ContainsAny(d.URL, "\r\n") {
			return "invalid monitor URL"
		}
		if e.Type == contracts.IncidentResolved && (d.ResolvedAt == nil || d.ResolvedAt.Before(*d.StartedAt) || d.DurationSeconds < 0) {
			return "invalid resolution"
		}
	default:
		return "unsupported event type"
	}
	return ""
}
func compose(e contracts.Event, publicURL string) (string, string) {
	d := e.Data
	stamp := e.OccurredAt.UTC().Format(time.RFC3339)
	if e.Type == contracts.VerificationRequested {
		return "[SitePulse] Подтвердите email", fmt.Sprintf("Подтвердите email для получения уведомлений SitePulse:\n\n%s\n\nСсылка действует до %s.\nЕсли вы не регистрировались, проигнорируйте письмо.\n", d.VerificationURL, d.ExpiresAt.UTC().Format(time.RFC3339))
	}
	status := "DOWN"
	details := "Причина: " + d.Reason
	if e.Type == contracts.IncidentResolved {
		status = "RECOVERED"
		details = fmt.Sprintf("Длительность инцидента: %d секунд", d.DurationSeconds)
	}
	return "[SitePulse] " + status + ": " + d.MonitorName, fmt.Sprintf("%s: %s\nURL: %s\nВремя события: %s\n%s\n\nМонитор: %s/monitors/%s\n", status, d.MonitorName, d.URL, stamp, details, publicURL, d.MonitorID)
}
func (s *Service) claim(ctx context.Context) (notification, error) {
	var n notification
	err := s.DB.QueryRow(ctx, `UPDATE notifications SET status='sending',attempts=attempts+1,lease_token=$1,lease_until=now()+interval '60 seconds' WHERE id=(SELECT id FROM notifications WHERE attempts<5 AND ((status IN ('pending','retry_wait') AND next_attempt_at<=now()) OR (status='sending' AND lease_until<now())) ORDER BY next_attempt_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING id,event_id,recipient_email,subject,body,lease_token,attempts`, platform.ID()).Scan(&n.ID, &n.EventID, &n.Recipient, &n.Subject, &n.Body, &n.Lease, &n.Attempts)
	return n, err
}

var errLeaseLost = errors.New("notification lease lost")
