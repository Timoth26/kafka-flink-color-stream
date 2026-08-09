FROM golang:1.26-alpine AS builder

WORKDIR /app
COPY . .

RUN go mod download && CGO_ENABLED=0 GOOS=linux go build -o /app/binary .

FROM alpine:3.24
WORKDIR /app
COPY --from=builder /app/binary .

CMD ["./binary"]