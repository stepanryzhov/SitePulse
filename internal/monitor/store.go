package monitor

import (
	"context"
	"errors"
	"net/mail"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"sitepulse/internal/contracts"
	"sitepulse/internal/platform"
)

type Store struct {
	DB        *pgxpool.Pool
	PublicURL string
}

func normalizeEmail(email string) (string, bool) {
	email = strings.ToLower(strings.TrimSpace(email))
	addr, err := mail.ParseAddress(email)
	return email, err == nil && addr.Address == email && len(email) <= 254 && !strings.ContainsAny(email, "\r\n")
}
func addEvent(ctx context.Context, tx pgx.Tx, topic, key string, e contracts.Event) error {
	data, err := e.JSON()
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events(id,topic,message_key,payload) VALUES($1,$2,$3,$4)`, e.ID, topic, key, data)
	return err
}
func (s *Store) verification(ctx context.Context, tx pgx.Tx, u User) error {
	raw := platform.Token()
	expires := time.Now().UTC().Add(24 * time.Hour)
	if _, err := tx.Exec(ctx, `UPDATE email_verification_tokens SET used_at=now() WHERE user_id=$1 AND used_at IS NULL`, u.ID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO email_verification_tokens(id,user_id,token_hash,expires_at) VALUES($1,$2,$3,$4)`, platform.ID(), u.ID, platform.Hash(raw), expires); err != nil {
		return err
	}
	e := contracts.New(contracts.VerificationRequested, contracts.Data{UserID: u.ID, RecipientEmail: &u.Email, VerificationURL: s.PublicURL + "/verify-email?token=" + raw, ExpiresAt: &expires})
	return addEvent(ctx, tx, contracts.AccountsTopic, u.ID, e)
}
func (s *Store) Register(ctx context.Context, email, password string) (User, error) {
	hash, err := hashPassword(password)
	if err != nil {
		return User{}, err
	}
	u := User{ID: platform.ID(), Email: email}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return u, err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO users(id,email,password_hash) VALUES($1,$2,$3)`, u.ID, email, hash)
	if err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "23505" {
			return u, ErrConflict
		}
		return u, err
	}
	if err = s.verification(ctx, tx, u); err != nil {
		return u, err
	}
	return u, tx.Commit(ctx)
}
func (s *Store) Login(ctx context.Context, email, password string) (User, error) {
	var u User
	err := s.DB.QueryRow(ctx, `SELECT id,email,password_hash,email_verified_at FROM users WHERE email=$1`, email).Scan(&u.ID, &u.Email, &u.PasswordHash, &u.VerifiedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		_, _ = hashPassword(password)
		return u, ErrCredentials
	}
	if err != nil {
		return u, err
	}
	if !checkPassword(password, u.PasswordHash) {
		return u, ErrCredentials
	}
	return u, nil
}
func (s *Store) NewSession(ctx context.Context, userID string) (string, error) {
	raw := platform.Token()
	_, err := s.DB.Exec(ctx, `INSERT INTO sessions(id,user_id,token_hash,expires_at) VALUES($1,$2,$3,now()+interval '30 days')`, platform.ID(), userID, platform.Hash(raw))
	return raw, err
}
func (s *Store) Session(ctx context.Context, raw string) (User, error) {
	var u User
	err := s.DB.QueryRow(ctx, `SELECT u.id,u.email,u.email_verified_at FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=$1 AND s.expires_at>now()`, platform.Hash(raw)).Scan(&u.ID, &u.Email, &u.VerifiedAt)
	return u, err
}
func (s *Store) Logout(ctx context.Context, raw string) error {
	_, err := s.DB.Exec(ctx, `DELETE FROM sessions WHERE token_hash=$1`, platform.Hash(raw))
	return err
}
func (s *Store) Verify(ctx context.Context, token string) error {
	if len(token) != 64 {
		return ErrToken
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Lock the user before the token, the same order used by resend.
	var userID string
	err = tx.QueryRow(ctx, `SELECT u.id FROM users u JOIN email_verification_tokens t ON t.user_id=u.id WHERE t.token_hash=$1 FOR UPDATE OF u`, platform.Hash(token)).Scan(&userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrToken
	}
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE email_verification_tokens SET used_at=now() WHERE token_hash=$1 AND used_at IS NULL AND expires_at>now()`, platform.Hash(token))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrToken
	}
	if _, err = tx.Exec(ctx, `UPDATE users SET email_verified_at=COALESCE(email_verified_at,now()) WHERE id=$1`, userID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) Resend(ctx context.Context, userID string) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var u User
	if err = tx.QueryRow(ctx, `SELECT id,email,email_verified_at FROM users WHERE id=$1 FOR UPDATE`, userID).Scan(&u.ID, &u.Email, &u.VerifiedAt); err != nil {
		return err
	}
	if u.VerifiedAt != nil {
		return nil
	}
	var recent bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM email_verification_tokens WHERE user_id=$1 AND created_at>now()-interval '1 minute')`, userID).Scan(&recent); err != nil {
		return err
	}
	if recent {
		return ErrRateLimit
	}
	if err = s.verification(ctx, tx, u); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

const monitorColumns = `id,user_id,name,url,interval_seconds,timeout_seconds,expected_status,status,is_paused,next_check_at,last_checked_at,last_response_ms,consecutive_failures,consecutive_successes,lease_token,created_at`

type scanner interface{ Scan(...any) error }

func scanMonitor(row scanner) (Monitor, error) {
	var m Monitor
	err := row.Scan(&m.ID, &m.UserID, &m.Name, &m.URL, &m.Interval, &m.Timeout, &m.ExpectedStatus, &m.Status, &m.Paused, &m.NextCheckAt, &m.LastCheckedAt, &m.LastResponseMS, &m.Failures, &m.Successes, &m.LeaseToken, &m.CreatedAt)
	return m, err
}
func (s *Store) Monitors(ctx context.Context, userID string) ([]Monitor, error) {
	rows, err := s.DB.Query(ctx, `SELECT `+monitorColumns+` FROM monitors WHERE user_id=$1 AND deleted_at IS NULL ORDER BY created_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Monitor{}
	for rows.Next() {
		m, err := scanMonitor(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
func (s *Store) Monitor(ctx context.Context, userID, id string) (Monitor, error) {
	if !platform.ValidID(id) {
		return Monitor{}, ErrNotFound
	}
	m, err := scanMonitor(s.DB.QueryRow(ctx, `SELECT `+monitorColumns+` FROM monitors WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL`, id, userID))
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	return m, err
}
func (s *Store) Create(ctx context.Context, userID string, in MonitorInput) (Monitor, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return Monitor{}, err
	}
	defer tx.Rollback(ctx)
	var lock string
	if err = tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, userID).Scan(&lock); err != nil {
		return Monitor{}, err
	}
	var count int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM monitors WHERE user_id=$1 AND deleted_at IS NULL`, userID).Scan(&count); err != nil {
		return Monitor{}, err
	}
	if count >= 20 {
		return Monitor{}, ErrLimit
	}
	m, err := scanMonitor(tx.QueryRow(ctx, `INSERT INTO monitors(id,user_id,name,url,interval_seconds,timeout_seconds,expected_status) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING `+monitorColumns, platform.ID(), userID, in.Name, in.URL, in.Interval, in.Timeout, in.ExpectedStatus))
	if err != nil {
		return m, err
	}
	return m, tx.Commit(ctx)
}
func (s *Store) Update(ctx context.Context, userID, id string, in MonitorInput) (Monitor, error) {
	m, err := scanMonitor(s.DB.QueryRow(ctx, `UPDATE monitors SET name=$3,url=$4,interval_seconds=$5,timeout_seconds=$6,expected_status=$7,lease_token=NULL,consecutive_failures=0,consecutive_successes=0,next_check_at=now(),updated_at=now() WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL RETURNING `+monitorColumns, id, userID, in.Name, in.URL, in.Interval, in.Timeout, in.ExpectedStatus))
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	return m, err
}
func (s *Store) Action(ctx context.Context, userID, id, action string) error {
	var query string
	switch action {
	case "pause":
		query = `UPDATE monitors SET is_paused=true,lease_token=NULL,consecutive_failures=0,consecutive_successes=0,updated_at=now() WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL`
	case "resume":
		query = `UPDATE monitors SET is_paused=false,next_check_at=now(),lease_token=NULL,consecutive_failures=0,consecutive_successes=0,updated_at=now() WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL`
	case "delete":
		query = `UPDATE monitors SET deleted_at=now(),lease_token=NULL,lease_until=NULL,updated_at=now() WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL`
	default:
		return ErrNotFound
	}
	tag, err := s.DB.Exec(ctx, query, id, userID)
	if err == nil && tag.RowsAffected() == 0 {
		err = ErrNotFound
	}
	return err
}
func (s *Store) Checks(ctx context.Context, id string, limit, offset int) ([]Check, error) {
	rows, err := s.DB.Query(ctx, `SELECT id,checked_at,response_time_ms,status_code,success,error_type FROM checks WHERE monitor_id=$1 ORDER BY checked_at DESC,id DESC LIMIT $2 OFFSET $3`, id, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Check{}
	for rows.Next() {
		var c Check
		if err := rows.Scan(&c.ID, &c.CheckedAt, &c.ResponseMS, &c.StatusCode, &c.Success, &c.ErrorType); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
func (s *Store) Incidents(ctx context.Context, id string, limit, offset int) ([]Incident, error) {
	rows, err := s.DB.Query(ctx, `SELECT id,started_at,resolved_at,cause,EXTRACT(EPOCH FROM (COALESCE(resolved_at,now())-started_at))::bigint FROM incidents WHERE monitor_id=$1 ORDER BY started_at DESC,id DESC LIMIT $2 OFFSET $3`, id, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Incident{}
	for rows.Next() {
		var i Incident
		if err := rows.Scan(&i.ID, &i.StartedAt, &i.ResolvedAt, &i.Cause, &i.DurationSeconds); err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}
func (s *Store) Stats(ctx context.Context, id, period string) (Stats, error) {
	days := map[string]int{"24h": 1, "7d": 7, "30d": 30}[period]
	out := Stats{Period: period}
	err := s.DB.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE success),avg(response_time_ms) FROM checks WHERE monitor_id=$1 AND checked_at>=now()-make_interval(days=>$2)`, id, days).Scan(&out.Total, &out.Successful, &out.AverageMS)
	if out.Total > 0 {
		value := 100 * float64(out.Successful) / float64(out.Total)
		out.Availability = &value
	}
	return out, err
}
