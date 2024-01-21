# Use a Rust base image for the building stage
FROM rust:latest as build

# Install dependencies required for Protocol Buffers
RUN apt-get update && apt-get install -y --no-install-recommends unzip curl \
    && PROTOC_ZIP=protoc-3.24.3-linux-x86_64.zip \  # Ensure this version matches your requirements
    && curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v3.24.3/$PROTOC_ZIP \
    && unzip -o $PROTOC_ZIP -d /usr/local bin/protoc \
    && rm -f $PROTOC_ZIP \
    && apt-get clean \  # Clean up to reduce image size
    && rm -rf /var/lib/apt/lists/*  # Clean up to reduce image size

# Copy the project files to the container
COPY . /root/datanode

# Set the working directory
WORKDIR /root/datanode

# Build the Rust application in release mode
RUN cargo build --release

# Use Ubuntu as the base image for the final image
FROM ubuntu:22.04 as final

# Expose the port the DataNode will run on
EXPOSE 50002

# Copy the compiled binary from the build stage to the final image
COPY --from=build /root/datanode/target/release/rs_hdfs_datanode /bin/rs_hdfs_datanode

# Ensure the binary is executable
RUN chmod +x /bin/rs_hdfs_datanode

RUN mkdir -p /hdfs/data

# Define the default command to run the DataNode
CMD ["/bin/rs_hdfs_datanode"]
