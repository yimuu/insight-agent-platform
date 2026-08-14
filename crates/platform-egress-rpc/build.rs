fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protocol = "../../proto/insight/platform/v1/egress_internal.proto";
    let include = "../../proto";
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut prost = tonic_prost_build::Config::new();
    prost.protoc_executable(protoc);
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_with_config(prost, &[protocol], &[include])?;
    println!("cargo:rerun-if-changed={protocol}");
    Ok(())
}
