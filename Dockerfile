FROM golang:1.17.6 as builder

WORKDIR /tmp/edge

COPY . .

ARG BUILDER
ARG VERSION

ENV EDGE_BUILDER=${BUILDER}
ENV EDGE_VERSION=${VERSION}

RUN apt-get update && apt-get install make git gcc -y && \
    make build_deps && \
    make

FROM alpine:latest

WORKDIR /app

COPY --from=builder /tmp/edge/bin/edge .

CMD ["/app/edge"]
