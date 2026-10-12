FROM golang:1.23-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /mini-redis-server ./cmd/mini-redis-server

FROM scratch
COPY --from=build /mini-redis-server /mini-redis-server
EXPOSE 6379
ENTRYPOINT ["/mini-redis-server", "-addr", "0.0.0.0:6379"]
