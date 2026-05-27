FROM golang:1.24-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY common ./common
RUN go mod download
COPY filter_payment_format .
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/filter_payment_format

FROM busybox:latest
COPY --from=builder /build/bin/filter_payment_format /filter_payment_format
ENTRYPOINT ["./filter_payment_format"]
