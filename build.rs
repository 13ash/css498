
fn main() {
    tonic_build::configure()
        .compile(
            &[
                "proto/client_protocol.proto",
                "proto/datanode_protocol.proto",
            ],
            &["proto"],
        )
        .unwrap();
}