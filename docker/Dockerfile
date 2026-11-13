# The emulator of THIS checkout, built from source.
#
# Build context: the repository root (docker-compose.yml sets it), filtered by
# Dockerfile.dockerignore beside this file so `target/` is never sent to the builder.
#
# The first build compiles every service crate of the workspace and takes a long time. The
# cargo registry and the build's own target directory are BuildKit cache mounts, so a build
# after a change recompiles only what the change touched.
FROM rust:1-trixie AS build

# aws-lc-sys and the embedded JavaScript engine compile C; aws-lc-sys drives it with cmake.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake clang \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p aws-emulator-router \
    && cp target/release/aws-emulator /usr/local/bin/aws-emulator

# The same Debian release the build stage links against, so the C library matches.
FROM debian:trixie-slim

# curl is the compose healthcheck's client.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /usr/local/bin/aws-emulator /usr/local/bin/aws-emulator

# Loopback is the default; a container is reached from outside itself.
ENV EMULATOR_BIND_ADDRESS=0.0.0.0
EXPOSE 9297

ENTRYPOINT ["aws-emulator"]
