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

FROM debian:bookworm-slim
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		ca-certificates \
		openssl \
	&& rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/moltrade-relayer /usr/local/bin/
COPY config.template.toml /etc/moltrade-relayer/
CMD ["moltrade-relayer", "--config", "/etc/moltrade-relayer/config.template.toml"]