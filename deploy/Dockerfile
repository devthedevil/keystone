# Keystone has no third-party dependencies, so the build stage needs no
# module download step and the runtime image needs nothing but the binary.
FROM golang:1.22-alpine AS build

ARG VERSION=dev
WORKDIR /src
COPY . .

# CGO off gives a static binary that runs on scratch.
RUN CGO_ENABLED=0 go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/keystoned  ./cmd/keystoned && \
    CGO_ENABLED=0 go build -ldflags "-s -w -X main.version=${VERSION}" -o /out/keystonectl ./cmd/keystonectl

# Run the tests in the image build so a broken commit cannot produce a
# publishable artifact.
RUN go vet ./... && go test ./... -count=1 -short -timeout 5m

FROM alpine:3.19

RUN apk add --no-cache ca-certificates tini && \
    addgroup -S keystone && adduser -S -G keystone keystone && \
    mkdir -p /var/lib/keystone && chown keystone:keystone /var/lib/keystone

COPY --from=build /out/keystoned  /usr/local/bin/keystoned
COPY --from=build /out/keystonectl /usr/local/bin/keystonectl

USER keystone
VOLUME /var/lib/keystone

# 8080 is the client plane, 8081 the peer plane. They are separate listeners
# so client load cannot starve replication heartbeats; keep them separate when
# publishing ports.
EXPOSE 8080 8081

ENV KEYSTONE_CLIENT_ADDR=:8080 \
    KEYSTONE_PEER_ADDR=:8081 \
    KEYSTONE_DATA_DIR=/var/lib/keystone \
    KEYSTONE_LOG_FORMAT=json

# tini reaps zombies and, more importantly here, forwards SIGTERM so the
# replica runs its drain sequence instead of being killed mid-commit.
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/keystoned"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/health/live || exit 1
