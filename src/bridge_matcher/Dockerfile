FROM golang:1.24-alpine3.23 AS builder
# USD Filter uses docker multistage builds feature https://docs.docker.com/develop/develop-images/multistage-build/
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY common ./common
RUN go mod download
COPY bridge_matcher .
# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/bridge_matcher

FROM busybox:latest
COPY --from=builder /build/bin/bridge_matcher /bridge_matcher
ENTRYPOINT ["./bridge_matcher"]
