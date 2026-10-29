package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Service           string
	Addr              string
	DatabaseURL       string
	PublicURL         string
	Brokers           []string
	KafkaTLS          bool
	KafkaUser         string
	KafkaPassword     string
	CheckWorkers      int
	DeliveryWorkers   int
	RetentionDays     int
	SchedulerInterval time.Duration
	SMTPHost          string
	SMTPPort          string
	SMTPUser          string
	SMTPPassword      string
	SMTPFrom          string
	SMTPTLS           string
	SecureCookies     bool
}

func Load(service string) (Config, error) {
	prefix := strings.ToUpper(service)
	c := Config{
		Service:       service,
		Addr:          env(prefix+"_HTTP_ADDR", ":8080"),
		DatabaseURL:   os.Getenv(prefix + "_DATABASE_URL"),
		PublicURL:     strings.TrimRight(env("MONITOR_PUBLIC_URL", "http://localhost:8080"), "/"),
		Brokers:       strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaUser:     os.Getenv("KAFKA_USERNAME"),
		KafkaPassword: os.Getenv("KAFKA_PASSWORD"),
		SMTPHost:      env("SMTP_HOST", "localhost"),
		SMTPPort:      env("SMTP_PORT", "1025"),
		SMTPUser:      os.Getenv("SMTP_USERNAME"),
		SMTPPassword:  os.Getenv("SMTP_PASSWORD"),
		SMTPFrom:      env("SMTP_FROM", "SitePulse <sitepulse@example.test>"),
		SMTPTLS:       env("SMTP_TLS_MODE", "none"),
	}
	var boolErr error
	c.KafkaTLS, boolErr = strconv.ParseBool(env("KAFKA_TLS", "false"))
	if boolErr != nil {
		return c, fmt.Errorf("invalid KAFKA_TLS")
	}

	if service == "notifier" {
		c.Addr = env("NOTIFIER_HTTP_ADDR", ":8081")
	}
	if c.DatabaseURL == "" {
		return c, fmt.Errorf("%s_DATABASE_URL is required", prefix)
	}
	u, err := url.Parse(c.PublicURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return c, fmt.Errorf("invalid MONITOR_PUBLIC_URL")
	}
	c.SecureCookies = u.Scheme == "https"
	if c.CheckWorkers, err = integer("MONITOR_CHECK_WORKERS", 20, 1, 100); err != nil {
		return c, err
	}
	if c.DeliveryWorkers, err = integer("NOTIFIER_DELIVERY_WORKERS", 2, 1, 20); err != nil {
		return c, err
	}
	if c.RetentionDays, err = integer("MONITOR_CHECK_RETENTION_DAYS", 30, 1, 365); err != nil {
		return c, err
	}
	c.SchedulerInterval, err = time.ParseDuration(env("MONITOR_SCHEDULER_INTERVAL", "5s"))
	if err != nil || c.SchedulerInterval < time.Second {
		return c, fmt.Errorf("invalid MONITOR_SCHEDULER_INTERVAL")
	}
	if c.SMTPTLS != "none" && c.SMTPTLS != "starttls" && c.SMTPTLS != "tls" {
		return c, fmt.Errorf("SMTP_TLS_MODE must be none, starttls or tls")
	}
	if service == "notifier" && c.SMTPTLS == "none" && c.SMTPHost != "mailpit" && c.SMTPHost != "localhost" && c.SMTPHost != "127.0.0.1" {
		return c, fmt.Errorf("external SMTP requires TLS")
	}
	if (c.KafkaUser == "") != (c.KafkaPassword == "") {
		return c, fmt.Errorf("both Kafka credentials are required")
	}
	return c, nil
}
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func integer(key string, fallback, min, max int) (int, error) {
	v, err := strconv.Atoi(env(key, strconv.Itoa(fallback)))
	if err != nil || v < min || v > max {
		return 0, fmt.Errorf("invalid %s (allowed %d..%d)", key, min, max)
	}
	return v, nil
}
