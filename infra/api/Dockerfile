FROM rust:1.86 AS builder

WORKDIR /build
COPY api/ .
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/datastream-rs /usr/local/bin/datastream-rs

EXPOSE 3000
ENTRYPOINT ["datastream-rs"]
