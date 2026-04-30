//! build.rs — compiles `proto/sync.proto` into Rust gRPC stubs via tonic-build.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(&["proto/sync.proto"], &["proto"])?;
    Ok(())
}
