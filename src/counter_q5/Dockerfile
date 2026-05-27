FROM golang:1.24-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY common ./common
RUN go mod download
COPY counter_q5 .
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/counter_q5

FROM busybox:latest
COPY --from=builder /build/bin/counter_q5 /counter_q5
ENTRYPOINT ["./counter_q5"]
