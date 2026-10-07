# syntax=docker/dockerfile:1

# Build on the runner's arch and cross-compile, instead of emulating the
# toolchain with QEMU.
FROM --platform=$BUILDPLATFORM golang:1.27 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

ARG TARGETOS TARGETARCH
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" \
    -o /out/yosegaki .

FROM scratch
# CA certificates are only for "connect" and "list"; "serve" does not need them.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/yosegaki /usr/local/bin/yosegaki
# scratch has no /etc/passwd, so use numeric IDs (the usual "nonroot").
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/yosegaki"]
CMD ["serve", "-addr", ":8080"]
