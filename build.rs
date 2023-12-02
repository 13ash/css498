fn main() {
    tonic_build::configure()
        .compile(
            &[
                "proto/transfer_protocol.proto",
                "proto/tpc_protocol.proto",
                "proto/common.proto"
            ],
            &["proto"],
        )
        .unwrap();
}