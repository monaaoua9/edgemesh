FROM golang:1.25 AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/ ./cmd/...

# load generator + mock cloud + cert tool, for staging
FROM gcr.io/distroless/static-debian12:nonroot AS tools
COPY --from=build /out/simulator /out/cloudmock /out/gencert /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/simulator"]

# default target
FROM gcr.io/distroless/static-debian12:nonroot AS gateway
COPY --from=build /out/gateway /gateway
VOLUME /data
EXPOSE 1883 9000 8080
HEALTHCHECK --interval=15s --timeout=4s --start-period=5s CMD ["/gateway", "healthcheck"]
ENTRYPOINT ["/gateway", "-data", "/data"]
