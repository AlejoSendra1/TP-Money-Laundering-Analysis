FROM golang:1.24-alpine3.23 AS builder
# Date Filter uses docker multistage builds feature https://docs.docker.com/develop/develop-images/multistage-build/
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY common ./common
RUN go mod download
COPY q5_date_filter .
# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/q5_date_filter

FROM busybox:latest
COPY --from=builder /build/bin/q5_date_filter /q5_date_filter
ENTRYPOINT ["./q5_date_filter"]
