package monitor

import (
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"sitepulse/internal/platform"
)

func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) error {
	host := r.RemoteAddr
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if !s.limiter.allow(host, 20, time.Minute) {
		return bad(429, "too many authentication attempts")
	}
	select {
	case s.authSlots <- struct{}{}:
		defer func() { <-s.authSlots }()
	default:
		return bad(429, "authentication busy; retry shortly")
	}
	var in struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if isAPI(r) {
		if err := decode(r, &in); err != nil {
			return err
		}
	} else {
		in.Email = r.PostForm.Get("email")
		in.Password = r.PostForm.Get("password")
	}
	email, ok := normalizeEmail(in.Email)
	if !ok || len(in.Password) < 10 || len(in.Password) > 128 {
		return bad(422, "use a valid email and a password of 10–128 bytes")
	}
	register := strings.HasSuffix(r.URL.Path, "register")
	var u User
	var err error
	if register {
		u, err = s.store.Register(r.Context(), email, in.Password)
	} else {
		u, err = s.store.Login(r.Context(), email, in.Password)
	}
	if err != nil {
		return err
	}
	raw, err := s.store.NewSession(r.Context(), u.ID)
	if err != nil {
		return err
	}
	// Replacing a session invalidates the old browser token.
	if c, e := r.Cookie(s.cookieName("session")); e == nil {
		_ = s.store.Logout(r.Context(), c.Value)
	}
	s.setCookie(w, "session", raw, 30*86400)
	if isAPI(r) {
		status := 200
		if register {
			status = 201
		}
		writeJSON(w, status, map[string]any{"user": u, "csrf_token": platform.Hash("csrf:" + raw)})
		return nil
	}
	http.Redirect(w, r, "/monitors", 303)
	return nil
}
func (s *Server) logout(w http.ResponseWriter, r *http.Request) error {
	if c, err := r.Cookie(s.cookieName("session")); err == nil {
		if err = s.store.Logout(r.Context(), c.Value); err != nil {
			return err
		}
	}
	s.setCookie(w, "session", "", -1)
	if isAPI(r) {
		w.WriteHeader(204)
	} else {
		http.Redirect(w, r, "/login", 303)
	}
	return nil
}
func (s *Server) resend(w http.ResponseWriter, r *http.Request) error {
	if err := s.store.Resend(r.Context(), currentUser(r).ID); err != nil {
		return err
	}
	if isAPI(r) {
		writeJSON(w, 202, map[string]string{"message": "verification requested"})
	} else {
		http.Redirect(w, r, "/account?message="+url.QueryEscape("Письмо подтверждения поставлено в очередь."), 303)
	}
	return nil
}
func (s *Server) list(w http.ResponseWriter, r *http.Request) error {
	monitors, err := s.store.Monitors(r.Context(), currentUser(r).ID)
	if err != nil {
		return err
	}
	if isAPI(r) {
		writeJSON(w, 200, monitors)
		return nil
	}
	return s.render(w, r, "monitors", page{Title: "Мониторы", Monitors: monitors})
}
func monitorInput(r *http.Request, defaults MonitorInput) (MonitorInput, error) {
	in := defaults
	if isAPI(r) {
		if err := decode(r, &in); err != nil {
			return in, err
		}
	} else {
		in.Name = r.PostForm.Get("name")
		in.URL = r.PostForm.Get("url")
		in.Interval, _ = strconv.Atoi(r.PostForm.Get("interval_seconds"))
		in.Timeout, _ = strconv.Atoi(r.PostForm.Get("timeout_seconds"))
		in.ExpectedStatus, _ = strconv.Atoi(r.PostForm.Get("expected_status"))
	}
	if err := validateInput(r.Context(), &in); err != nil {
		return in, bad(422, err.Error())
	}
	return in, nil
}
func (s *Server) create(w http.ResponseWriter, r *http.Request) error {
	in, err := monitorInput(r, MonitorInput{Interval: 60, Timeout: 5, ExpectedStatus: 200})
	if err != nil {
		return err
	}
	m, err := s.store.Create(r.Context(), currentUser(r).ID, in)
	if err != nil {
		return err
	}
	if isAPI(r) {
		w.Header().Set("Location", "/api/v1/monitors/"+m.ID)
		writeJSON(w, 201, m)
	} else {
		http.Redirect(w, r, "/monitors/"+m.ID, 303)
	}
	return nil
}
func (s *Server) own(r *http.Request) (Monitor, error) {
	return s.store.Monitor(r.Context(), currentUser(r).ID, chi.URLParam(r, "id"))
}
func (s *Server) detail(w http.ResponseWriter, r *http.Request) error {
	m, err := s.own(r)
	if err != nil {
		return err
	}
	if isAPI(r) {
		writeJSON(w, 200, m)
		return nil
	}
	limit, offset, p, err := pagination(r)
	if err != nil {
		return err
	}
	checks, err := s.store.Checks(r.Context(), m.ID, limit, offset)
	if err != nil {
		return err
	}
	incidents, err := s.store.Incidents(r.Context(), m.ID, limit, offset)
	if err != nil {
		return err
	}
	period, err := statsPeriod(r)
	if err != nil {
		return err
	}
	stats, err := s.store.Stats(r.Context(), m.ID, period)
	if err != nil {
		return err
	}
	next := 0
	if len(checks) == limit || len(incidents) == limit {
		next = p + 1
	}
	return s.render(w, r, "detail", page{Title: m.Name, Monitor: m, Checks: checks, Incidents: incidents, Stats: stats, Period: period, Page: p, NextPage: next, PreviousPage: p - 1})
}
func (s *Server) editForm(w http.ResponseWriter, r *http.Request) error {
	m, err := s.own(r)
	if err != nil {
		return err
	}
	return s.render(w, r, "form", page{Title: "Настройки монитора", Monitor: m, Input: MonitorInput{m.Name, m.URL, m.Interval, m.Timeout, m.ExpectedStatus}})
}
func (s *Server) update(w http.ResponseWriter, r *http.Request) error {
	m, err := s.own(r)
	if err != nil {
		return err
	}
	in, err := monitorInput(r, MonitorInput{m.Name, m.URL, m.Interval, m.Timeout, m.ExpectedStatus})
	if err != nil {
		return err
	}
	m, err = s.store.Update(r.Context(), currentUser(r).ID, m.ID, in)
	if err != nil {
		return err
	}
	if isAPI(r) {
		writeJSON(w, 200, m)
	} else {
		http.Redirect(w, r, "/monitors/"+m.ID, 303)
	}
	return nil
}
func (s *Server) action(w http.ResponseWriter, r *http.Request, action string) error {
	m, err := s.own(r)
	if err != nil {
		return err
	}
	if err = s.store.Action(r.Context(), currentUser(r).ID, m.ID, action); err != nil {
		return err
	}
	if isAPI(r) {
		w.WriteHeader(204)
	} else {
		target := "/monitors/" + m.ID
		if action == "delete" {
			target = "/monitors"
		}
		http.Redirect(w, r, target, 303)
	}
	return nil
}
func pagination(r *http.Request) (limit, offset, pageNumber int, err error) {
	limit = 20
	pageNumber = 1
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil || limit < 1 || limit > 100 {
			return 0, 0, 0, bad(400, "limit must be 1–100")
		}
	}
	if v := r.URL.Query().Get("page"); v != "" {
		pageNumber, err = strconv.Atoi(v)
		if err != nil || pageNumber < 1 || pageNumber > 10000 {
			return 0, 0, 0, bad(400, "page must be 1–10000")
		}
	}
	return limit, (pageNumber - 1) * limit, pageNumber, nil
}
func statsPeriod(r *http.Request) (string, error) {
	p := r.URL.Query().Get("period")
	if p == "" {
		p = "24h"
	}
	if p != "24h" && p != "7d" && p != "30d" {
		return "", bad(400, "period must be 24h, 7d or 30d")
	}
	return p, nil
}
func (s *Server) checks(w http.ResponseWriter, r *http.Request) error {
	m, err := s.own(r)
	if err != nil {
		return err
	}
	limit, offset, p, err := pagination(r)
	if err != nil {
		return err
	}
	items, err := s.store.Checks(r.Context(), m.ID, limit, offset)
	if err != nil {
		return err
	}
	writeJSON(w, 200, map[string]any{"items": items, "page": p, "limit": limit})
	return nil
}
func (s *Server) incidents(w http.ResponseWriter, r *http.Request) error {
	m, err := s.own(r)
	if err != nil {
		return err
	}
	limit, offset, p, err := pagination(r)
	if err != nil {
		return err
	}
	items, err := s.store.Incidents(r.Context(), m.ID, limit, offset)
	if err != nil {
		return err
	}
	writeJSON(w, 200, map[string]any{"items": items, "page": p, "limit": limit})
	return nil
}
func (s *Server) stats(w http.ResponseWriter, r *http.Request) error {
	m, err := s.own(r)
	if err != nil {
		return err
	}
	p, err := statsPeriod(r)
	if err != nil {
		return err
	}
	stats, err := s.store.Stats(r.Context(), m.ID, p)
	if err != nil {
		return err
	}
	writeJSON(w, 200, stats)
	return nil
}
