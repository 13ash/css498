fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile(
            &[
                "proto/client_protocol.proto",
                "proto/datanode_protocol.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
