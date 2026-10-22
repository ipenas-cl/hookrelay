# syntax=docker/dockerfile:1
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/hookrelay /usr/local/bin/hookrelay
ENV HOOKRELAY_LISTEN=0.0.0.0:8080 HOOKRELAY_DB=/data/hookrelay.db
VOLUME ["/data"]
EXPOSE 8080
USER nonroot
ENTRYPOINT ["/usr/local/bin/hookrelay"]
