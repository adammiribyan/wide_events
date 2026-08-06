FROM --platform=$BUILDPLATFORM golang:1.24-bookworm AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=0.0.0-dev

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        gcc-aarch64-linux-gnu g++-aarch64-linux-gnu \
        gcc-x86-64-linux-gnu g++-x86-64-linux-gnu \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src/store
COPY store/go.mod store/go.sum ./
RUN go mod download
COPY store/ ./
RUN case "$TARGETARCH" in \
        amd64) export CC=x86_64-linux-gnu-gcc CXX=x86_64-linux-gnu-g++ ;; \
        arm64) export CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ ;; \
        *) echo "unsupported target architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && CGO_ENABLED=1 GOOS=$TARGETOS GOARCH=$TARGETARCH \
        go build -trimpath -ldflags "-s -w -X main.version=$VERSION" \
        -o /out/wide-events ./cmd/wide-events

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 wide-events \
    && useradd --uid 1000 --gid 1000 --no-create-home --shell /usr/sbin/nologin wide-events \
    && install -d -m 0700 -o 1000 -g 1000 /var/lib/wide-events /run/wide-events

COPY --from=build /out/wide-events /usr/local/bin/wide-events

USER 1000:1000
EXPOSE 7421
ENTRYPOINT ["/usr/local/bin/wide-events"]
