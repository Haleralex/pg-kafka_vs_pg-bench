FROM golang:1.25.7-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/bench-api ./cmd/api

FROM alpine:3.23
RUN addgroup -S bench && adduser -S -G bench bench
COPY --from=build /out/bench-api /usr/local/bin/bench-api
USER bench
EXPOSE 8080
ENTRYPOINT ["bench-api"]
