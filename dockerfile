FROM golang:1.12 as builder
WORKDIR /root/
COPY . /root/
RUN go mod tidy
RUN go build -o rabbitmq_test .

FROM alpine:latest as prod
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY *.yaml /root/
COPY --from=builder /root/rabbitmq_test .
ENTRYPOINT  ["./rabbitmq_test"]
