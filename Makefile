COMPOSE = docker compose --env-file .env -f deploy/compose.yaml

.PHONY: up down logs infra build check fmt migrate run-monitor run-notifier
.env:
	cp .env.example .env
up: .env
	$(COMPOSE) up -d --build
infra: .env
	$(COMPOSE) up -d postgres kafka kafka-init mailpit
down: .env
	$(COMPOSE) down
logs: .env
	$(COMPOSE) logs -f monitor-service notifier-service
build:
	go build -o bin/monitor ./cmd/monitor
	go build -o bin/notifier ./cmd/notifier
	go build -o bin/migrate ./cmd/migrate
check:
	go vet ./...
	go build ./...
fmt:
	gofmt -w cmd internal migrations web/embed.go
migrate: .env
	$(COMPOSE) run --rm --build migrate
run-monitor:
	go run ./cmd/monitor
run-notifier:
	go run ./cmd/notifier
