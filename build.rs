fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile(
            &[
                "proto/rshdfs_datanode_protocol.proto",
                "proto/rshdfs_namenode_protocol.proto",
                "datanode_namenode_protocol.proto",
                "proto/common.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
