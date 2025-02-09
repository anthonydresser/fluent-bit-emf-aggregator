FROM golang:1.21

WORKDIR /app
COPY go.mod .
RUN go mod download

COPY main.go .
RUN go build -o main .

CMD ["./main"]
