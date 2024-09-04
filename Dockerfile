# Build - first phase. 
FROM golang:1.22-alpine as builder
RUN mkdir /build
WORKDIR /build
COPY . .
RUN apk add --no-cache git

RUN go mod download
# Build the services
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags="-s -w" -o bsp-agent ./cmd/bspagent
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags="-s -w" -o bsp-extractor ./scripts/extractor.go
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags="-s -w" -o bsp-extractor-2 ./scripts/replica/extractor2.go
# Runtime/test -  second phase.
FROM alpine:3.20
RUN mkdir /app
WORKDIR /app
RUN apk update && apk add --no-cache bash git
RUN mkdir -p bin/block-ethereum bin/block-elrond
COPY --from=builder /build/bsp-agent /app
COPY --from=builder /build/entry.sh /app
COPY --from=builder /build/data /app/data
COPY --from=builder /build/codec /app/codec
COPY --from=builder /build/bsp-extractor /app
COPY --from=builder /build/bsp-extractor-2 /app

ENTRYPOINT [ "/bin/bash", "-l", "-c" ]
CMD [ "./entry.sh" ]
EXPOSE 8080
