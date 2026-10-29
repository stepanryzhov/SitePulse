package monitor

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"sitepulse/internal/config"
	"sitepulse/internal/platform"
	"sitepulse/web"
)

type requestKey int

const (
	userKey requestKey = iota
	csrfKey
)

type Server struct {
	store     *Store
	cfg       config.Config
	log       *slog.Logger
	templates *template.Template
	limiter   *rateLimiter
	authSlots chan struct{}
}
type page struct {
	Title, Error, Message, CSRF  string
	User                         *User
	Monitors                     []Monitor
	Monitor                      Monitor
	Checks                       []Check
	Incidents                    []Incident
	Stats                        Stats
	Input                        MonitorInput
	Period                       string
	Page, NextPage, PreviousPage int
}
type httpError struct {
	status  int
	message string
}

func (e *httpError) Error() string         { return e.message }
func bad(status int, message string) error { return &httpError{status, message} }

type endpoint func(http.ResponseWriter, *http.Request) error

func NewServer(store *Store, cfg config.Config, log *slog.Logger) (*Server, error) {
	funcs := template.FuncMap{
		"stamp": func(v any) string {
			switch t := v.(type) {
			case time.Time:
				return t.UTC().Format("02 Jan 2006 15:04:05 UTC")
			case *time.Time:
				if t != nil {
					return t.UTC().Format("02 Jan 2006 15:04:05 UTC")
				}
			}
			return "—"
		},
		"number": func(v any) string {
			switch n := v.(type) {
			case *int:
				if n != nil {
					return strconv.Itoa(*n)
				}
			case *int64:
				if n != nil {
					return strconv.FormatInt(*n, 10)
				}
			case *float64:
				if n != nil {
					return fmt.Sprintf("%.2f", *n)
				}
			}
			return "N/A"
		},
		"text": func(s *string) string {
			if s == nil {
				return "—"
			}
			return *s
		},
		"status": func(m Monitor) string {
			if m.Paused {
				return "paused"
			}
			return m.Status
		},
	}
	t, err := template.New("").Funcs(funcs).ParseFS(web.Files, "templates/*.html")
	if err != nil {
		return nil, err
	}
	return &Server{store: store, cfg: cfg, log: log, templates: t, limiter: &rateLimiter{entries: map[string]rateEntry{}}, authSlots: make(chan struct{}, 4)}, nil
}
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.Recoverer, s.security, s.session)
	r.Get("/health/live", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	r.Get("/health/ready", s.wrap(func(w http.ResponseWriter, r *http.Request) error {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.store.DB.Ping(ctx); err != nil {
			return bad(503, "database unavailable")
		}
		writeJSON(w, 200, map[string]string{"status": "ready"})
		return nil
	}))
	r.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.FS(web.Static()))))
	r.Get("/", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/monitors", http.StatusSeeOther) })
	r.Get("/api/v1/auth/csrf", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"csrf_token": csrf(r)})
	})
	for _, path := range []string{"/login", "/register"} {
		path := path
		r.Get(path, s.wrap(func(w http.ResponseWriter, r *http.Request) error {
			if currentUser(r) != nil {
				http.Redirect(w, r, "/monitors", 303)
				return nil
			}
			return s.render(w, r, strings.TrimPrefix(path, "/"), page{Title: strings.TrimPrefix(path, "/")})
		}))
		r.Post(path, s.wrap(s.authenticate))
	}
	r.Post("/api/v1/auth/register", s.wrap(s.authenticate))
	r.Post("/api/v1/auth/login", s.wrap(s.authenticate))
	r.Get("/verify-email", s.wrap(func(w http.ResponseWriter, r *http.Request) error {
		err := s.store.Verify(r.Context(), r.URL.Query().Get("token"))
		if err != nil {
			return err
		}
		return s.render(w, r, "message", page{Title: "Email подтверждён", Message: "Адрес подтверждён. Теперь вы будете получать уведомления об инцидентах."})
	}))
	r.Group(func(r chi.Router) {
		r.Use(s.requireUser)
		r.Post("/logout", s.wrap(s.logout))
		r.Post("/api/v1/auth/logout", s.wrap(s.logout))
		r.Get("/account", s.wrap(func(w http.ResponseWriter, r *http.Request) error {
			return s.render(w, r, "account", page{Title: "Аккаунт", Message: r.URL.Query().Get("message")})
		}))
		r.Post("/account/resend-verification", s.wrap(s.resend))
		r.Post("/api/v1/auth/resend-verification", s.wrap(s.resend))
		r.Get("/monitors", s.wrap(s.list))
		r.Get("/api/v1/monitors", s.wrap(s.list))
		r.Get("/monitors/new", s.wrap(func(w http.ResponseWriter, r *http.Request) error {
			return s.render(w, r, "form", page{Title: "Новый монитор", Input: MonitorInput{Interval: 60, Timeout: 5, ExpectedStatus: 200}})
		}))
		r.Post("/monitors", s.wrap(s.create))
		r.Post("/api/v1/monitors", s.wrap(s.create))
		r.Get("/monitors/{id}", s.wrap(s.detail))
		r.Get("/api/v1/monitors/{id}", s.wrap(s.detail))
		r.Get("/monitors/{id}/edit", s.wrap(s.editForm))
		r.Post("/monitors/{id}/edit", s.wrap(s.update))
		r.Patch("/api/v1/monitors/{id}", s.wrap(s.update))
		for _, action := range []string{"pause", "resume", "delete"} {
			action := action
			r.Post("/monitors/{id}/"+action, s.wrap(func(w http.ResponseWriter, r *http.Request) error { return s.action(w, r, action) }))
			if action != "delete" {
				r.Post("/api/v1/monitors/{id}/"+action, s.wrap(func(w http.ResponseWriter, r *http.Request) error { return s.action(w, r, action) }))
			}
		}
		r.Delete("/api/v1/monitors/{id}", s.wrap(func(w http.ResponseWriter, r *http.Request) error { return s.action(w, r, "delete") }))
		r.Get("/api/v1/monitors/{id}/checks", s.wrap(s.checks))
		r.Get("/api/v1/monitors/{id}/incidents", s.wrap(s.incidents))
		r.Get("/api/v1/monitors/{id}/stats", s.wrap(s.stats))
	})
	r.NotFound(func(w http.ResponseWriter, r *http.Request) { s.fail(w, r, bad(404, "page not found")) })
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) { s.fail(w, r, bad(405, "method not allowed")) })
	return r
}
func (s *Server) security(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		// Keep the origin on form POSTs without exposing paths or verification tokens.
		w.Header().Set("Referrer-Policy", "strict-origin")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'self'; img-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
		w.Header().Set("Cache-Control", "no-store")
		if s.cfg.SecureCookies {
			w.Header().Set("Strict-Transport-Security", "max-age=31536000")
		}
		r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
		next.ServeHTTP(w, r)
	})
}
func (s *Server) cookieName(base string) string {
	if s.cfg.SecureCookies {
		return "__Host-sitepulse_" + base
	}
	return "sitepulse_" + base
}
func (s *Server) setCookie(w http.ResponseWriter, name, value string, maxAge int) {
	http.SetCookie(w, &http.Cookie{Name: s.cookieName(name), Value: value, Path: "/", HttpOnly: true, Secure: s.cfg.SecureCookies, SameSite: http.SameSiteLaxMode, MaxAge: maxAge})
}
func (s *Server) session(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		raw := ""
		if c, err := r.Cookie(s.cookieName("session")); err == nil && len(c.Value) == 64 {
			u, err := s.store.Session(ctx, c.Value)
			if err == nil {
				ctx = context.WithValue(ctx, userKey, &u)
				raw = c.Value
			} else if !errors.Is(err, pgx.ErrNoRows) {
				s.fail(w, r, bad(503, "session store unavailable"))
				return
			}
		}
		if raw == "" {
			if c, err := r.Cookie(s.cookieName("csrf")); err == nil && len(c.Value) == 64 {
				raw = c.Value
			} else {
				raw = platform.Token()
				s.setCookie(w, "csrf", raw, 86400)
			}
		}
		ctx = context.WithValue(ctx, csrfKey, platform.Hash("csrf:"+raw))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
func currentUser(r *http.Request) *User { u, _ := r.Context().Value(userKey).(*User); return u }
func csrf(r *http.Request) string       { v, _ := r.Context().Value(csrfKey).(string); return v }
func (s *Server) requireUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if currentUser(r) == nil {
			if isAPI(r) {
				s.fail(w, r, bad(401, "authentication required"))
			} else {
				http.Redirect(w, r, "/login", 303)
			}
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (s *Server) wrap(fn endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" && r.Method != "HEAD" {
			if err := s.checkCSRF(r); err != nil {
				s.fail(w, r, err)
				return
			}
		}
		if err := fn(w, r); err != nil {
			s.fail(w, r, err)
		}
	}
}
func (s *Server) checkCSRF(r *http.Request) error {
	origin := r.Header.Get("Origin")
	if origin != "" && origin != s.cfg.PublicURL {
		return bad(403, "origin is not allowed")
	}
	if origin == "" && r.Header.Get("Referer") != "" {
		u, err := url.Parse(r.Header.Get("Referer"))
		if err != nil || u.Scheme+"://"+u.Host != s.cfg.PublicURL {
			return bad(403, "origin is not allowed")
		}
	}
	if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		return bad(403, "cross-site request rejected")
	}
	token := r.Header.Get("X-CSRF-Token")
	if !isAPI(r) {
		if err := r.ParseForm(); err != nil {
			return bad(400, "invalid form")
		}
		token = r.PostForm.Get("csrf_token")
	}
	if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(csrf(r))) != 1 {
		return bad(403, "invalid CSRF token; reload the form or GET /api/v1/auth/csrf")
	}
	return nil
}
func isAPI(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/health/")
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func decode(r *http.Request, dst any) error {
	if strings.Split(r.Header.Get("Content-Type"), ";")[0] != "application/json" {
		return bad(400, "Content-Type must be application/json")
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return bad(400, "invalid JSON body")
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return bad(400, "expected one JSON object")
	}
	return nil
}
func (s *Server) render(w http.ResponseWriter, r *http.Request, name string, p page) error {
	p.User = currentUser(r)
	p.CSRF = csrf(r)
	var body bytes.Buffer
	if err := s.templates.ExecuteTemplate(&body, name, p); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, err := body.WriteTo(w)
	return err
}
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	status := 500
	message := "internal server error"
	var he *httpError
	switch {
	case errors.As(err, &he):
		status = he.status
		message = he.message
	case errors.Is(err, ErrNotFound):
		status = 404
		message = err.Error()
	case errors.Is(err, ErrConflict):
		status = 409
		message = err.Error()
	case errors.Is(err, ErrLimit):
		status = 422
		message = err.Error()
	case errors.Is(err, ErrCredentials):
		status = 401
		message = err.Error()
	case errors.Is(err, ErrRateLimit):
		status = 429
		message = err.Error()
	case errors.Is(err, ErrToken):
		status = 400
		message = err.Error()
	}
	if status >= 500 {
		s.log.Error("request failed", "request_id", middleware.GetReqID(r.Context()), "status", status)
	}
	if status == 429 {
		w.Header().Set("Retry-After", "60")
	}
	if isAPI(r) {
		writeJSON(w, status, map[string]string{"error": message})
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = s.render(w, r, "message", page{Title: "Ошибка", Error: message})
}
