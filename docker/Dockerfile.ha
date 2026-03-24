# pg_facets Docker Build
# Multi-stage build for PostgreSQL extension with Zig

# =============================================================================
# Stage 1: Builder Stage
# =============================================================================
FROM ubuntu:22.04 AS builder

# Environment variables
ENV DEBIAN_FRONTEND=noninteractive \
    PG_MAJOR=17 \
    ZIG_VERSION=0.15.2

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        ca-certificates \
        gnupg2 \
        git \
        cmake \
        gcc \
        libc-dev \
        make \
        pkg-config \
        libssl-dev \
        wget \
        xz-utils && \
    rm -rf /var/lib/apt/lists/*

# Add PostgreSQL Apt repository and install PostgreSQL development packages
RUN curl -sSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    echo "deb http://apt.postgresql.org/pub/repos/apt/ jammy-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        postgresql-server-dev-$PG_MAJOR && \
    rm -rf /var/lib/apt/lists/*

# Install Zig (multi-arch) and lock Zig build target to GNU libc.
ARG TARGETARCH=amd64
RUN MACHINE="$(uname -m)" && \
    case "${MACHINE}" in \
      aarch64|arm64) RUNTIME_ARCH=arm64;; \
      x86_64|amd64) RUNTIME_ARCH=amd64;; \
      *) RUNTIME_ARCH="${TARGETARCH}";; \
    esac && \
    if [ "${TARGETARCH}" != "${RUNTIME_ARCH}" ]; then \
      echo "WARN: TARGETARCH=${TARGETARCH} differs from runtime arch ${RUNTIME_ARCH}; using runtime arch."; \
    fi && \
    case "${RUNTIME_ARCH}" in \
      arm64) ZIG_ARCH=aarch64-linux; ZIG_TARGET=aarch64-linux-gnu;; \
      *) ZIG_ARCH=x86_64-linux; ZIG_TARGET=x86_64-linux-gnu;; \
    esac && \
    printf '%s' "${ZIG_TARGET}" > /tmp/zig_target && \
    cd /tmp && \
    wget https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-${ZIG_VERSION}.tar.xz && \
    tar -xf zig-${ZIG_ARCH}-${ZIG_VERSION}.tar.xz && \
    mv zig-${ZIG_ARCH}-${ZIG_VERSION} /usr/local/zig && \
    ln -s /usr/local/zig/zig /usr/local/bin/zig

# Clone, patch, and build pg_roaringbitmap (no submodule required)
ARG PG_ROARINGBITMAP_REPO=https://github.com/ChenHuajun/pg_roaringbitmap.git
ARG PG_ROARINGBITMAP_REF=master
RUN git clone --depth 1 --branch "${PG_ROARINGBITMAP_REF}" "${PG_ROARINGBITMAP_REPO}" /tmp/pg_roaringbitmap && \
    if ! grep -q 'non-x86 fallback for pg_facets integration' /tmp/pg_roaringbitmap/roaring.c; then \
      printf '%s\n' '' '// non-x86 fallback for pg_facets integration' '#if !defined(__x86_64__) && !defined(_M_AMD64)' 'int croaring_hardware_support(void) { return 0; }' '#endif' >> /tmp/pg_roaringbitmap/roaring.c; \
    fi && \
    cd /tmp/pg_roaringbitmap && \
    make clean && \
    make && \
    make install && \
    make clean && \
    rm -rf .git

# Copy pg_facets source and build
WORKDIR /tmp/pg_facets
COPY src/ ./src/
COPY sql/ ./sql/
COPY build.zig ./
COPY pg_facets.control ./
RUN mkdir -p deps && cp -R /tmp/pg_roaringbitmap ./deps/pg_roaringbitmap

RUN ZIG_TARGET="$(tr -d '\n' </tmp/zig_target)" && \
    zig build -Dtarget="${ZIG_TARGET}" -Doptimize=ReleaseFast

# =============================================================================
# Stage 2: Final Image
# =============================================================================
FROM timescale/timescaledb-ha:pg17.2-ts2.17.2-oss

ENV DEBIAN_FRONTEND=noninteractive \
    PG_MAJOR=17

# Copy pg_roaringbitmap extension
COPY --from=builder /usr/lib/postgresql/$PG_MAJOR/lib/roaringbitmap.so /usr/lib/postgresql/$PG_MAJOR/lib/roaringbitmap.so
COPY --from=builder /usr/share/postgresql/$PG_MAJOR/extension/roaringbitmap* /usr/share/postgresql/$PG_MAJOR/extension/

# Copy pg_facets extension
COPY --from=builder /tmp/pg_facets/zig-out/lib/libpg_facets.so /usr/lib/postgresql/$PG_MAJOR/lib/pg_facets.so
COPY --from=builder /tmp/pg_facets/pg_facets.control /usr/share/postgresql/$PG_MAJOR/extension/
# Copy all SQL files for version support
COPY --from=builder /tmp/pg_facets/sql/pg_facets--*.sql /usr/share/postgresql/$PG_MAJOR/extension/

# Copy initialization scripts
COPY docker/init/01-init-postgres.sql /docker-entrypoint-initdb.d/
COPY --chmod=755 docker/init/02-init-template1.sh /docker-entrypoint-initdb.d/

# Copy PostgreSQL configuration
COPY docker/config/postgresql.conf /var/lib/postgresql/data/postgresql.conf

# Copy test file for validation
COPY test/sql/complete_test.sql /usr/share/postgresql/$PG_MAJOR/extension/pg_facets_test.sql

EXPOSE 5432
CMD ["postgres"]

