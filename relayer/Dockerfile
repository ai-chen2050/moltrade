FROM rust:1.89.0 as builder
WORKDIR /app
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		clang \
		libclang-dev \
		llvm-dev \
		build-essential \
		pkg-config \
		cmake \
		libssl-dev \
	&& rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /app/target/release/moltrade-relayer /usr/local/bin/
COPY config.template.toml /etc/moltrade-relayer/
CMD ["moltrade-relayer", "--config", "/etc/moltrade-relayer/config.template.toml"]