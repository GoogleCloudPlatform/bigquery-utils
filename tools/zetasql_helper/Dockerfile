FROM debian:unstable as builder

ENV DEBIAN_FRONTEND noninteractive

# Set up basic environment
RUN apt-get update \
    && apt-get install -y curl gnupg gcc git python3 make

# Install Bazel
RUN curl -f https://bazel.build/bazel-release.pub.gpg | apt-key add - \
 && echo "deb [arch=amd64] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list

RUN apt update && apt install -y bazel-1.0.0 \
  && rm -rf /var/lib/apt/lists/*

ENV WORKSPACE /src
WORKDIR $WORKSPACE

# Copy the source code inside the builder
COPY . .

# Build the c++ server
RUN bazel-1.0.0 build //zetasql_helper/local_service:run_server


## deploy stage
FROM debian:unstable-slim
ENV WORKSPACE /app
WORKDIR WORKSPACE

COPY --from=builder /src/bazel-bin/zetasql_helper/local_service/run_server .
CMD ["./run_server"]

