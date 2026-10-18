FROM ubuntu:24.04 AS build
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential cmake git ca-certificates \
      libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DKV_BUILD_TESTS=OFF \
    && cmake --build build -j"$(nproc)" --target kvd kvctl kvbench

FROM ubuntu:24.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      libgrpc++1.51t64 libprotobuf32t64 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home kv \
    && mkdir -p /home/kv/data && chown kv:kv /home/kv/data   # volumes inherit this ownership
COPY --from=build /src/build/kvd /src/build/kvctl /src/build/kvbench /usr/local/bin/
USER kv
WORKDIR /home/kv
ENTRYPOINT ["kvd"]
