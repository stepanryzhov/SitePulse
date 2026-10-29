package notifier

import (
	"context"
	"errors"
	"net/textproto"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *Service) RunDelivery(ctx context.Context, workers int) {
	var wg sync.WaitGroup
	// Shared ticker caps this process at one SMTP attempt per second.
	throttle := time.NewTicker(time.Second)
	defer throttle.Stop()
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-throttle.C:
				}
				if ctx.Err() != nil {
					return
				}
				n, err := s.claim(ctx)
				if errors.Is(err, pgx.ErrNoRows) {
					continue
				}
				if err != nil {
					if ctx.Err() == nil {
						s.Log.Warn("claim notification failed")
					}
					continue
				}
				work, cancel := context.WithTimeout(context.WithoutCancel(ctx), 25*time.Second)
				sendErr := s.Mailer.Send(work, n)
				// Preserve the result even when SMTP used its full deadline.
				cancel()
				save, saveCancel := context.WithTimeout(context.Background(), 5*time.Second)
				err = s.finish(save, n, sendErr)
				saveCancel()
				if err != nil {
					s.Log.Error("save delivery result failed", "event_id", n.EventID)
				} else {
					s.Log.Info("delivery attempted", "event_id", n.EventID, "attempt", n.Attempts, "accepted", sendErr == nil)
				}
			}
		}()
	}
	wg.Wait()
}
func (s *Service) finish(ctx context.Context, n notification, sendErr error) error {
	status := "sent"
	var failure *string
	var sentAt *time.Time
	next := time.Now().UTC()
	if sendErr == nil {
		sentAt = &next
	} else {
		description := "temporary SMTP failure"
		status = "retry_wait"
		var smtpErr *textproto.Error
		if errors.As(sendErr, &smtpErr) && smtpErr.Code >= 500 {
			status = "failed"
			description = "permanent SMTP rejection"
		}
		if n.Attempts >= 5 {
			status = "failed"
			description = "SMTP attempt limit reached"
		}
		failure = &description
		delays := []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour, time.Hour}
		next = next.Add(delays[n.Attempts-1])
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE notifications SET status=$3,last_error=$4,next_attempt_at=$5,sent_at=$6,lease_token=NULL,lease_until=NULL WHERE id=$1 AND lease_token=$2 AND status='sending'`, n.ID, n.Lease, status, failure, next, sentAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errLeaseLost
	}
	if _, err = tx.Exec(ctx, `INSERT INTO notification_attempts(notification_id,attempt,error,success) VALUES($1,$2,$3,$4)`, n.ID, n.Attempts, failure, sendErr == nil); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Service) RunCleanup(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		work, cancel := context.WithTimeout(ctx, 15*time.Second)
		_, err := s.DB.Exec(work, `UPDATE notifications SET status='failed',lease_token=NULL,lease_until=NULL,last_error='attempt limit reached after interrupted delivery' WHERE status='sending' AND attempts>=5 AND lease_until<now()`)
		if err == nil {
			_, err = s.DB.Exec(work, `UPDATE notifications SET body='[redacted after retention]' WHERE status IN ('sent','failed') AND created_at<now()-interval '7 days' AND body<>'[redacted after retention]'`)
		}
		if err == nil {
			_, err = s.DB.Exec(work, `DELETE FROM notification_attempts WHERE attempted_at<now()-interval '30 days'`)
		}
		if err == nil {
			_, err = s.DB.Exec(work, `DELETE FROM notifications WHERE status IN ('sent','failed') AND created_at<now()-interval '30 days'`)
		}
		if err == nil {
			_, err = s.DB.Exec(work, `DELETE FROM rejected_events WHERE created_at<now()-interval '30 days'`)
		}
		cancel()
		if err != nil && ctx.Err() == nil {
			s.Log.Warn("notification cleanup failed")
		}
		timer.Reset(time.Minute)
	}
}
