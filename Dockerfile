FROM golang:1.25-alpine

RUN apk add --no-cache git bash

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go install github.com/air-verse/air@latest

EXPOSE 8080

CMD ["air", "-c", "/app/air.toml"]