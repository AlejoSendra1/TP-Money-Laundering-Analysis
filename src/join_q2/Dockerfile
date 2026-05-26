FROM golang:1.24-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY common ./common
RUN go mod download
COPY join_q2 .
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/join_q2

FROM busybox:latest
COPY --from=builder /build/bin/join_q2 /join_q2
ENTRYPOINT ["./join_q2"]

