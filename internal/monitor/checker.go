package monitor

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"sitepulse/internal/platform"
)

var blockedNetworks = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"), netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("224.0.0.0/4"), netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001::/23"), netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2002::/16"), netip.MustParsePrefix("3fff::/20"),
}

func publicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	if ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip) {
		return false
	}
	for _, prefix := range blockedNetworks {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}
func resolvePublic(ctx context.Context, host string) ([]netip.Addr, error) {
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("host has no addresses")
	}
	for _, ip := range ips {
		if !publicIP(ip) {
			return nil, fmt.Errorf("only public addresses are allowed")
		}
	}
	return ips, nil
}
func ValidateURL(ctx context.Context, raw string) error {
	if len(raw) > 2048 || strings.ContainsAny(raw, "\r\n\t") {
		return fmt.Errorf("URL is too long or contains control characters")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return fmt.Errorf("use an absolute HTTP/HTTPS URL without credentials or fragment")
	}
	if port := u.Port(); port != "" && port != "80" && port != "443" {
		return fmt.Errorf("only ports 80 and 443 are allowed")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := resolvePublic(ctx, u.Hostname()); err != nil {
		return fmt.Errorf("URL must resolve to public IP addresses")
	}
	return nil
}
func validateInput(ctx context.Context, in *MonitorInput) error {
	in.Name = strings.TrimSpace(in.Name)
	in.URL = strings.TrimSpace(in.URL)
	if len(in.Name) < 1 || len(in.Name) > 120 || strings.ContainsAny(in.Name, "\r\n") {
		return fmt.Errorf("name must contain 1–120 characters without newlines")
	}
	if in.Interval != 60 && in.Interval != 300 && in.Interval != 600 {
		return fmt.Errorf("interval must be 60, 300 or 600 seconds")
	}
	if in.Timeout < 1 || in.Timeout > 15 {
		return fmt.Errorf("timeout must be 1–15 seconds")
	}
	if in.ExpectedStatus < 100 || in.ExpectedStatus > 599 {
		return fmt.Errorf("expected status must be 100–599")
	}
	return ValidateURL(ctx, in.URL)
}
func NewChecker() *http.Client {
	transport := &http.Transport{
		Proxy: nil, MaxIdleConns: 100, MaxIdleConnsPerHost: 2, MaxConnsPerHost: 20, IdleConnTimeout: 60 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 15 * time.Second, MaxResponseHeaderBytes: 32 << 10,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, ForceAttemptHTTP2: true,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			if port != "80" && port != "443" {
				return nil, fmt.Errorf("port forbidden")
			}
			ips, err := resolvePublic(ctx, host)
			if err != nil {
				return nil, err
			}
			dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
			var last error
			for _, ip := range ips {
				conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(ip.Unmap().String(), port))
				if err == nil {
					return conn, nil
				}
				last = err
			}
			return nil, last
		},
	}
	return &http.Client{Transport: transport, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
}
func performCheck(ctx context.Context, client *http.Client, m Monitor) Check {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(m.Timeout)*time.Second)
	defer cancel()
	result := Check{ID: platform.ID(), CheckedAt: time.Now().UTC()}
	started := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.URL, nil)
	if err == nil {
		req.Header.Set("User-Agent", "SitePulse/1.0")
		var response *http.Response
		response, err = client.Do(req)
		if err == nil {
			code := response.StatusCode
			result.StatusCode = &code
			_, _ = io.CopyN(io.Discard, response.Body, 4096)
			_ = response.Body.Close()
			result.Success = code == m.ExpectedStatus && ctx.Err() == nil
			if !result.Success {
				kind := "http_status"
				if ctx.Err() != nil {
					kind = "timeout"
				}
				result.ErrorType = &kind
			}
		}
	}
	result.ResponseMS = time.Since(started).Milliseconds()
	if err != nil {
		kind := classifyError(err)
		result.ErrorType = &kind
	}
	return result
}
func classifyError(err error) string {
	var ne net.Error
	var dns *net.DNSError
	var cert *tls.CertificateVerificationError
	var invalid x509.CertificateInvalidError
	var record tls.RecordHeaderError
	var op *net.OpError
	switch {
	case errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()):
		return "timeout"
	case errors.As(err, &dns):
		return "dns"
	case errors.As(err, &cert) || errors.As(err, &invalid) || errors.As(err, &record):
		return "tls"
	case errors.As(err, &op):
		return "connection"
	default:
		return "other"
	}
}
