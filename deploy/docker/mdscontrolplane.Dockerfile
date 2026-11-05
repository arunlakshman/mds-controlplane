# Image for the control plane (cmd/mdscontrolplane).
# Build from the repo root:
#   docker build -f deploy/docker/mdscontrolplane.Dockerfile -t mdscontrolplane:dev .
#
# The *.pb.go and *_gen.go files are not in git, so this build makes them first
# (buf, then ops/tools/wiregen).
# Versions match mise.toml (buf) and the Makefile (protoc plugins).

FROM bufbuild/buf:1.47.2 AS buf

FROM golang:1.26 AS build
COPY --from=buf /usr/local/bin/buf /usr/local/bin/buf
ENV GOBIN=/usr/local/bin
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.2 \
 && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN cd internal/controlplane/api && buf generate \
 && cd /src && go run ./ops/tools/wiregen
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mdscontrolplane ./cmd/mdscontrolplane

# Static binary, no shell, runs as a non-root user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/mdscontrolplane /mdscontrolplane
EXPOSE 8080 9101
ENTRYPOINT ["/mdscontrolplane"]
