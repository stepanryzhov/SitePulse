package monitor

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"sitepulse/internal/contracts"
	"sitepulse/internal/platform"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (s *Store) claim(ctx context.Context) (Monitor, error) {
	return scanMonitor(s.DB.QueryRow(ctx, `UPDATE monitors SET lease_token=$1,lease_until=now()+make_interval(secs=>timeout_seconds+30) WHERE id=(SELECT id FROM monitors WHERE NOT is_paused AND deleted_at IS NULL AND next_check_at<=now() AND (lease_until IS NULL OR lease_until<now()) ORDER BY next_check_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING `+monitorColumns, platform.ID()))
}
func (s *Store) saveCheck(ctx context.Context, m Monitor, check Check) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	current, err := scanMonitor(tx.QueryRow(ctx, `SELECT `+monitorColumns+` FROM monitors WHERE id=$1 AND lease_token=$2 AND NOT is_paused AND deleted_at IS NULL FOR UPDATE`, m.ID, m.LeaseToken))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	oldStatus := current.Status
	if check.Success {
		current.Successes++
		current.Failures = 0
		if current.Status == "unknown" || (current.Status == "down" && current.Successes >= 2) {
			current.Status = "up"
		}
	} else {
		current.Failures++
		current.Successes = 0
		if current.Failures >= 3 {
			current.Status = "down"
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO checks(id,monitor_id,checked_at,response_time_ms,status_code,success,error_type) VALUES($1,$2,$3,$4,$5,$6,$7)`, check.ID, m.ID, check.CheckedAt, check.ResponseMS, check.StatusCode, check.Success, check.ErrorType)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE monitors SET status=$2,consecutive_failures=$3,consecutive_successes=$4,last_checked_at=$5,last_response_ms=$6,next_check_at=$5::timestamptz+make_interval(secs=>interval_seconds),lease_token=NULL,lease_until=NULL,updated_at=now() WHERE id=$1`, m.ID, current.Status, current.Failures, current.Successes, check.CheckedAt, check.ResponseMS)
	if err != nil {
		return err
	}
	if oldStatus != current.Status && (current.Status == "down" || oldStatus == "down") {
		var recipient *string
		if err = tx.QueryRow(ctx, `SELECT CASE WHEN email_verified_at IS NOT NULL THEN email END FROM users WHERE id=$1`, m.UserID).Scan(&recipient); err != nil {
			return err
		}
		now := time.Now().UTC()
		data := contracts.Data{MonitorID: m.ID, MonitorName: m.Name, URL: m.URL, RecipientEmail: recipient}
		kind := contracts.IncidentOpened
		if current.Status == "down" {
			data.IncidentID = platform.ID()
			data.StartedAt = &now
			data.Reason = "other"
			if check.ErrorType != nil {
				data.Reason = *check.ErrorType
			}
			_, err = tx.Exec(ctx, `INSERT INTO incidents(id,monitor_id,started_at,cause) VALUES($1,$2,$3,$4)`, data.IncidentID, m.ID, now, data.Reason)
		} else {
			kind = contracts.IncidentResolved
			var started time.Time
			err = tx.QueryRow(ctx, `UPDATE incidents SET resolved_at=$2 WHERE monitor_id=$1 AND resolved_at IS NULL RETURNING id,started_at,cause`, m.ID, now).Scan(&data.IncidentID, &started, &data.Reason)
			data.StartedAt = &started
			data.ResolvedAt = &now
			data.DurationSeconds = int64(now.Sub(started).Seconds())
		}
		if err != nil {
			return err
		}
		if err = addEvent(ctx, tx, contracts.IncidentsTopic, m.ID, contracts.New(kind, data)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func (s *Store) RunScheduler(ctx context.Context, client *http.Client, workers int, interval time.Duration, log *slog.Logger) {
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			timer := time.NewTimer(0)
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
				}
				m, err := s.claim(ctx)
				if errors.Is(err, pgx.ErrNoRows) {
					timer.Reset(interval)
					continue
				}
				if err != nil {
					if ctx.Err() == nil {
						log.Error("claim check failed")
					}
					timer.Reset(interval)
					continue
				}
				// The shutdown context stops claims; an in-flight check gets its own bounded deadline.
				work, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(m.Timeout+5)*time.Second)
				check := performCheck(work, client, m)
				err = s.saveCheck(work, m, check)
				cancel()
				if err != nil {
					log.Error("save check failed", "monitor_id", m.ID)
				} else {
					log.Info("check complete", "monitor_id", m.ID, "duration_ms", check.ResponseMS, "success", check.Success, "error_type", check.ErrorType)
				}
				timer.Reset(interval)
			}
		}()
	}
	wg.Wait()
	client.CloseIdleConnections()
}
func (s *Store) RunOutbox(ctx context.Context, client *kgo.Client, log *slog.Logger) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		// A transaction-scoped global lock serializes publishers across replicas, preserving sequence.
		err := s.publishBatch(ctx, client)
		if err != nil && ctx.Err() == nil {
			log.Warn("outbox publish failed; will retry")
		}
	}
}
func (s *Store) publishBatch(ctx context.Context, client *kgo.Client) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var acquired bool
	if err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(1936289901)`).Scan(&acquired); err != nil {
		return err
	}
	if !acquired {
		return nil
	}
	rows, err := tx.Query(ctx, `SELECT id,topic,message_key,payload FROM outbox_events WHERE published_at IS NULL ORDER BY sequence LIMIT 20`)
	if err != nil {
		return err
	}
	type pending struct {
		id, topic, key string
		payload        []byte
	}
	events := []pending{}
	for rows.Next() {
		var e pending
		if err = rows.Scan(&e.id, &e.topic, &e.key, &e.payload); err != nil {
			rows.Close()
			return err
		}
		events = append(events, e)
	}
	rows.Close()
	if rows.Err() != nil {
		return rows.Err()
	}
	for _, e := range events {
		result := client.ProduceSync(ctx, &kgo.Record{Topic: e.topic, Key: []byte(e.key), Value: e.payload}).FirstErr()
		if result != nil {
			_, err = tx.Exec(ctx, `UPDATE outbox_events SET attempts=attempts+1,last_error='broker delivery failed' WHERE id=$1`, e.id)
			if err != nil {
				return err
			}
			if err = tx.Commit(ctx); err != nil {
				return err
			}
			return result
		}
		if _, err = tx.Exec(ctx, `UPDATE outbox_events SET published_at=now(),attempts=attempts+1,last_error=NULL,payload='{}'::jsonb WHERE id=$1`, e.id); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func (s *Store) RunCleanup(ctx context.Context, days int, log *slog.Logger) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		cleanup, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := s.cleanupChecks(cleanup, days)
		if err == nil {
			_, err = s.DB.Exec(cleanup, `DELETE FROM sessions WHERE expires_at<now()`)
		}
		if err == nil {
			_, err = s.DB.Exec(cleanup, `DELETE FROM email_verification_tokens WHERE expires_at<now()`)
		}
		if err == nil {
			_, err = s.DB.Exec(cleanup, `DELETE FROM outbox_events WHERE published_at<now()-interval '7 days'`)
		}
		cancel()
		if err != nil && ctx.Err() == nil {
			log.Warn("cleanup failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Store) cleanupChecks(ctx context.Context, days int) error {
	for {
		tag, err := s.DB.Exec(ctx, `DELETE FROM checks WHERE id IN (
   SELECT id FROM checks WHERE checked_at < now()-make_interval(days=>$1) LIMIT 10000
  )`, days)
		if err != nil {
			return err
		}
		if tag.RowsAffected() < 10000 {
			return nil
		}
	}
}
