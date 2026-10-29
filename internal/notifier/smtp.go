package notifier

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/smtp"
	"strings"
	"time"

	"sitepulse/internal/config"
)

type Mailer struct {
	cfg  config.Config
	from *mail.Address
}

func NewMailer(cfg config.Config) (*Mailer, error) {
	from, err := mail.ParseAddress(cfg.SMTPFrom)
	if err != nil || strings.ContainsAny(cfg.SMTPFrom, "\r\n") {
		return nil, fmt.Errorf("invalid SMTP_FROM")
	}
	return &Mailer{cfg: cfg, from: from}, nil
}
func (m *Mailer) Send(ctx context.Context, n notification) error {
	c := m.cfg
	address := net.JoinHostPort(c.SMTPHost, c.SMTPPort)
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	defer conn.Close()
	deadline := time.Now().Add(20 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err = conn.SetDeadline(deadline); err != nil {
		return err
	}
	tlsConfig := &tls.Config{ServerName: c.SMTPHost, MinVersion: tls.VersionTLS12}
	if c.SMTPTLS == "tls" {
		secured := tls.Client(conn, tlsConfig)
		if err = secured.HandshakeContext(ctx); err != nil {
			return err
		}
		conn = secured
	}
	client, err := smtp.NewClient(conn, c.SMTPHost)
	if err != nil {
		return err
	}
	defer client.Close()
	if c.SMTPTLS == "starttls" {
		if err = client.StartTLS(tlsConfig); err != nil {
			return err
		}
	}
	if c.SMTPUser != "" {
		if err = client.Auth(smtp.PlainAuth("", c.SMTPUser, c.SMTPPassword, c.SMTPHost)); err != nil {
			return err
		}
	}
	if err = client.Mail(m.from.Address); err != nil {
		return err
	}
	if err = client.Rcpt(n.Recipient); err != nil {
		return err
	}
	writer, err := client.Data()
	if err != nil {
		return err
	}
	if _, err = writer.Write(m.message(n)); err != nil {
		_ = writer.Close()
		return err
	}
	if err = writer.Close(); err != nil {
		return err
	}
	// DATA acceptance is success; a failed QUIT must not cause another delivery.
	_ = client.Quit()
	return nil
}
func (m *Mailer) message(n notification) []byte {
	var b bytes.Buffer
	subject := strings.NewReplacer("\r", " ", "\n", " ").Replace(n.Subject)
	fmt.Fprintf(&b, "From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\nMessage-ID: <%s@sitepulse>\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n", m.from.String(), (&mail.Address{Address: n.Recipient}).String(), mime.QEncoding.Encode("utf-8", subject), time.Now().UTC().Format(time.RFC1123Z), n.ID)
	qp := quotedprintable.NewWriter(&b)
	_, _ = qp.Write([]byte(n.Body))
	_ = qp.Close()
	return b.Bytes()
}
