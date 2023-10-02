FROM golang:1.20
COPY .. /app
WORKDIR /app
RUN go build -o client ./cmd/client/client.go
CMD ./client
