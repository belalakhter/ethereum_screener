FROM golang:1.23.3-alpine

WORKDIR /app

COPY . .

RUN go mod download
RUN go build -o main ./cmd/main.go

EXPOSE 8080

CMD ["./main"]
