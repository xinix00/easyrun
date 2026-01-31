FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /easyraft ./cmd/election

FROM alpine:3.19
COPY --from=builder /easyraft /easyraft
ENTRYPOINT ["/easyraft"]
CMD ["-config", "/etc/easyraft/config.yaml"]
