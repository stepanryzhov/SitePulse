FROM golang:1.26.5-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/monitor ./cmd/monitor \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/notifier ./cmd/notifier \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate
FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata && addgroup -S sitepulse && adduser -S -G sitepulse sitepulse
COPY --from=build /out/ /app/
USER sitepulse
WORKDIR /app
EXPOSE 8080 8081
CMD ["/app/monitor"]
