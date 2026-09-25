# Multi-stage build. TARGET selects which cmd/ binary to build:
# server (default), worker, outbox-dispatcher.
ARG TARGET=server

FROM golang:1.26-bookworm AS build
ARG TARGET
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/app ./cmd/${TARGET}

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
ARG TARGET
WORKDIR /app
COPY --from=build /out/app /app/app
COPY --from=build /src/migrations /app/migrations
USER nonroot:nonroot
EXPOSE 8084
ENTRYPOINT ["/app/app"]
