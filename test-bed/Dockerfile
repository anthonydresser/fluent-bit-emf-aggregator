FROM golang:1.21

WORKDIR /app
COPY main.go .
RUN go mod init main && \
    go build -o main .

CMD ["./main", "-port", "5170", "-rps", "16000"]
