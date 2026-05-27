FROM golang:1.24-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY common ./common
RUN go mod download
COPY currencies_cache .
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/currencies_cache

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /build/bin/currencies_cache /currencies_cache
ENTRYPOINT ["./currencies_cache"]
