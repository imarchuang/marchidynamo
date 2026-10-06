# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY store ./store
COPY server ./server
COPY cmd ./cmd
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/marchidynamo ./cmd/marchidynamo

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl \
	&& adduser -D -u 10001 marchi \
	&& mkdir -p /data \
	&& chown marchi:marchi /data
COPY --from=build /out/marchidynamo /usr/local/bin/marchidynamo
USER marchi
WORKDIR /data
VOLUME ["/data"]
EXPOSE 8001 8002 8003
ENTRYPOINT ["marchidynamo"]
