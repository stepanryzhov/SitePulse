# SitePulse

Мониторинг публичных HTTP/HTTPS-сайтов на Go. Два независимых сервиса, PostgreSQL, Kafka и email через SMTP. Интерфейс — серверный HTML без JavaScript и сборки frontend.

**Что работает:** регистрация и вход, подтверждение email, до 20 мониторов на пользователя, проверки каждые 60/300/600 секунд, пауза и удаление, история и статистика за 24 часа/7/30 дней, инциденты и письма DOWN/RECOVERED. Проверки хранятся 30 дней. Тестовые файлы намеренно не добавлены по условию проекта; CI проверяет форматирование, `go vet` и сборку.

## Быстрый запуск

Нужны Docker с Compose v2. Для запуска исходников отдельно — Go 1.26.0 или новее.

```sh
make up
```

Команда создаёт `.env` из `.env.example`, собирает бинарники, запускает инфраструктуру и оба сервиса. При первом старте скачивание образов и подготовка Kafka могут занять несколько минут. Миграции применяются автоматически при старте каждого сервиса; повторный запуск безопасен.

- Приложение: http://localhost:8080
- Письма в Mailpit: http://localhost:8025

Зарегистрируйтесь с любым email, откройте письмо в Mailpit, перейдите по ссылке подтверждения и добавьте, например, `https://example.com`. Сначала монитор имеет состояние UNKNOWN, затем начнёт проверяться в течение ближайшего цикла планировщика. Страницы обновляются обычной перезагрузкой браузера.

```sh
make logs       # Логи обоих Go-сервисов
make down       # Остановка, данные сохраняются
make up         # Повторный запуск / пересборка
make migrate    # Явно применить миграции обеих баз
make build      # Локальные бинарники в bin/
make check      # go vet и go build, без тестов
make fmt        # gofmt
```

Локальные порты привязаны к `127.0.0.1`. Kafka доступна приложениям на хосте через `localhost:19092`, контейнерам — через `kafka:9092`. PostgreSQL — `localhost:5433` (`POSTGRES_PORT`), SMTP Mailpit — `localhost:1025`. Notifier не публикует свой технический HTTP-порт наружу. Если порты заняты другими проектами, освободите их или измените проброс в Compose и соответствующую конфигурацию.

Удаление volumes через `docker compose --env-file .env -f deploy/compose.yaml down -v` уничтожает локальные данные. Изменение паролей в `.env` после создания PostgreSQL volume само по себе не меняет пароли ролей БД; их нужно изменить в PostgreSQL отдельно.

## Разработка на хосте

```sh
make infra
# .env — доверенный локальный файл, образец совместим с shell.
set -a
. ./.env
set +a
go run ./cmd/migrate
# В двух терминалах с загруженными переменными:
go run ./cmd/monitor
go run ./cmd/notifier
```

Приложения читают окружение процесса, а не `.env` напрямую. Compose читает `.env` через Makefile. HTML, CSS и SQL-миграции встроены в бинарники через `go:embed`.

## Как устроен проект

```text
cmd/monitor/       HTTP, scheduler и outbox publisher
cmd/notifier/      Kafka consumer и SMTP delivery
cmd/migrate/       Явное применение миграций
internal/config/   Проверка env-конфигурации
internal/platform/ PostgreSQL, Kafka, криптографические токены, HTTP server
internal/contracts/ Общий envelope событий Kafka
internal/monitor/  Аккаунты, SQL, HTTP API, SSRF checker, фоновые задачи
internal/notifier/ Consumer, SMTP, доставка и повторы
migrations/        Раздельные миграции двух сервисов
web/               html/template и CSS
deploy/            Dockerfile, Compose и создание баз
```

Подробности транзакций, состояний и отказов — в [docs/architecture.md](docs/architecture.md). Доступ к SQL прямой через `pgxpool`; ORM, контейнеров зависимостей и дополнительных слоёв нет.

## JSON API

Все маршруты `/api/v1/monitors` требуют cookie-сессию. Изменяющие запросы требуют заголовок `X-CSRF-Token`, включая регистрацию и вход. Первичный токен выдаёт `GET /api/v1/auth/csrf` с cookie. Вход и регистрация возвращают новый токен, привязанный к созданной сессии. Не используйте старый токен после смены сессии. Если отправляете `Origin`, он должен точно совпадать с `MONITOR_PUBLIC_URL`.

Пример (нужен `jq`):

```sh
BASE=http://localhost:8080
COOKIE=/tmp/sitepulse-cookies
CSRF=$(curl -fsS -c "$COOKIE" "$BASE/api/v1/auth/csrf" | jq -r .csrf_token)
AUTH=$(curl -fsS -b "$COOKIE" -c "$COOKIE" \
  -H 'Content-Type: application/json' -H "X-CSRF-Token: $CSRF" \
  -d '{"email":"alex@example.com","password":"example-password-123"}' \
  "$BASE/api/v1/auth/register")
CSRF=$(printf '%s' "$AUTH" | jq -r .csrf_token)
curl -fsS -b "$COOKIE" -H 'Content-Type: application/json' \
  -H "X-CSRF-Token: $CSRF" \
  -d '{"name":"Example","url":"https://example.com","interval_seconds":60,"timeout_seconds":5,"expected_status":200}' \
  "$BASE/api/v1/monitors"
curl -fsS -b "$COOKIE" "$BASE/api/v1/monitors"
```

| Метод | Путь | Действие |
|---|---|---|
| GET | `/api/v1/auth/csrf` | CSRF-токен |
| POST | `/api/v1/auth/register` | `{email,password}`, создаёт сессию, 201 |
| POST | `/api/v1/auth/login` | `{email,password}`, создаёт сессию |
| POST | `/api/v1/auth/logout` | Удаляет сессию, 204 |
| POST | `/api/v1/auth/resend-verification` | Новое письмо, 202 |
| GET | `/api/v1/monitors` | Массив собственных мониторов |
| POST | `/api/v1/monitors` | Создание, 201 + Location |
| GET | `/api/v1/monitors/{id}` | Детали |
| PATCH | `/api/v1/monitors/{id}` | Частичное изменение настроек |
| POST | `/api/v1/monitors/{id}/pause` | Пауза, 204 |
| POST | `/api/v1/monitors/{id}/resume` | Возобновление, 204 |
| DELETE | `/api/v1/monitors/{id}` | Soft delete, 204 |
| GET | `/api/v1/monitors/{id}/checks?page=1&limit=20` | История, `{items,page,limit}` |
| GET | `/api/v1/monitors/{id}/incidents?page=1&limit=20` | Инциденты и длительность |
| GET | `/api/v1/monitors/{id}/stats?period=7d` | `24h`, `7d`, `30d` |
| GET | `/health/live` | Процесс отвечает |
| GET | `/health/ready` | PostgreSQL доступна |

При создании нужны `name` и `url`; значения по умолчанию: `interval_seconds=60`, `timeout_seconds=5`, `expected_status=200`. PATCH сохраняет пропущенные поля. Лимит страницы 1–100, номер страницы 1–10000. Ошибки имеют вид `{"error":"message"}`; используются 400, 401, 403, 404, 409, 422, 429, 500, 503. Тело запроса ограничено 16 КиБ. Пустая статистика возвращает `null` для процентов и среднего ответа; это не 100% доступности.

## Конфигурация

Все значения — через env. Значения Compose предназначены для локального запуска. Для внешнего SMTP запускайте сервис с нужными env или используйте собственный Compose override: локальный Compose намеренно направляет письма в Mailpit.

| Переменная | По умолчанию / смысл |
|---|---|
| `MONITOR_DATABASE_URL` | Обязательно, своя PostgreSQL БД |
| `NOTIFIER_DATABASE_URL` | Обязательно, своя PostgreSQL БД |
| `MONITOR_HTTP_ADDR` | `:8080` |
| `NOTIFIER_HTTP_ADDR` | `:8081`, технические health endpoints |
| `MONITOR_PUBLIC_URL` | `http://localhost:8080`, без завершающего `/` и подпути |
| `MONITOR_CHECK_WORKERS` | `20`, допустимо 1–100 |
| `MONITOR_SCHEDULER_INTERVAL` | `5s`, не менее 1s |
| `MONITOR_CHECK_RETENTION_DAYS` | `30`, допустимо 1–365 |
| `NOTIFIER_DELIVERY_WORKERS` | `2`, допустимо 1–20, общий лимит 1 SMTP-попытка/сек на процесс |
| `KAFKA_BROKERS` | `localhost:9092`, список через запятую |
| `KAFKA_TLS` | `false`, `true` включает TLS с системными CA |
| `KAFKA_USERNAME`, `KAFKA_PASSWORD` | Вместе включают SASL PLAIN; в production использовать с TLS |
| `SMTP_HOST`, `SMTP_PORT` | `localhost`, `1025` |
| `SMTP_USERNAME`, `SMTP_PASSWORD` | Необязательная SMTP AUTH PLAIN |
| `SMTP_FROM` | `SitePulse <sitepulse@example.test>` |
| `SMTP_TLS_MODE` | `none`, `starttls` или `tls` |

`SMTP_TLS_MODE=none` разрешён только для локального `mailpit`, `localhost`, `127.0.0.1`. Для внешнего SMTP обязательна проверка TLS-сертификата. Укажите HTTPS в `MONITOR_PUBLIC_URL` для Secure cookies с префиксом `__Host-`. При работе за reverse proxy он должен обслуживать тот же публичный origin и передавать запросы по доверенной сети. Приложение не доверяет произвольному `X-Forwarded-For`; встроенный лимит входа использует адрес непосредственного клиента. Настройте отдельный лимит по реальному IP на reverse proxy.

## Доставка и эксплуатация

- UNKNOWN → UP после первой успешной проверки, UNKNOWN/UP → DOWN после 3 ошибок, DOWN → UP после 2 успехов. Неожиданный HTTP-код, включая редирект, считается ошибкой, если он не совпадает с настроенным ожидаемым кодом. Автоматического перехода по редиректам нет.
- Пауза останавливает новые проверки, сбрасывает счётчики и инвалидирует текущую аренду. Подтверждённое состояние и открытый инцидент сохраняются. Возобновление назначает проверку сразу; письмо о восстановлении приходит только после подтверждения успехами. Изменение настроек также сбрасывает счётчики и аренду. Soft delete скрывает монитор и останавливает проверки без дополнительных писем.
- Состояние, инцидент и outbox сохраняются атомарно. При недоступной Kafka outbox накапливается; scheduler продолжает работу. Readiness зависит от PostgreSQL, поэтому недоступность Kafka или SMTP не останавливает monitor.
- Не подтверждённый email отсутствует в инцидентном событии. Notifier учитывает событие без создания письма. После подтверждения старые пропущенные уведомления не рассылаются.
- Дедупликация `event_id` и запись письма происходят в одной транзакции; offset Kafka коммитится после неё. Некорректные события попадают в `rejected_events` без сырого payload.
- Повторы SMTP: через 1 минуту, 5 минут, 15 минут и 1 час, максимум 5 попыток. Явный постоянный SMTP-отказ 5xx завершает задание сразу. История в `notification_attempts`; истёкшая аренда восстанавливает прерванную отправку.
- **Exactly-once email не гарантируется.** Если SMTP принял письмо, а запись `sent` не сохранилась, письмо может прийти повторно. `sent` означает принятие SMTP-сервером, а не попадание во входящие. Письма с повторами могут приходить не по порядку.
- PostgreSQL и Kafka используют volumes. Неопубликованные события не удаляются. После публикации payload outbox очищается; запись хранится 7 дней. Тела завершённых писем редактируются через 7 дней, завершённые задания, попытки и rejected events удаляются через 30 дней. Идентификаторы обработанных событий сохраняются для дедупликации; Kafka retention — 7 дней. Очистка checks идёт каждый час порциями по 10000 строк в пределах 30-секундного окна.
- Логи JSON через `slog` не содержат паролей, cookies, полных payload и ссылок подтверждения. Результаты доставки видны в БД Notifier. Для диагностики: `docker compose --env-file .env -f deploy/compose.yaml exec postgres psql -U postgres -d notifier_db`, затем `SELECT id,event_id,status,attempts,next_attempt_at,last_error FROM notifications;`.

Для публичного размещения нужны свои секреты, HTTPS, резервные копии PostgreSQL, закрытые сетевые порты БД/Kafka, Kafka TLS/ACL и настройка SPF/DKIM/DMARC почтового домена. Проверка URL запрещает приватные и специальные адреса и повторяет DNS-проверку при установлении соединения; дополнительно ограничьте исходящий трафик на уровне хоста. MVP не содержит организаций, SMS, Telegram, графиков и публичных status pages.
