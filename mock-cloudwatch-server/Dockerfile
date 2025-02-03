FROM golang:1.21

WORKDIR /app
COPY main.go .
RUN go mod init main && \
    go build -o main .

CMD ["./main"]
