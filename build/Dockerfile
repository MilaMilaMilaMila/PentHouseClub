FROM golang:1.20
COPY ../cmd/client /app/cmd/client
COPY ../go.* /app
WORKDIR /app
RUN go build -o client ./cmd/client/client.go
