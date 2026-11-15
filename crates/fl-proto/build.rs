//! Generates Rust code from the `.proto` files.
//!
//! The build uses the `protoc` named by the `PROTOC` environment variable if
//! it is set. Otherwise it uses the copy bundled in `protoc-bin-vendored`,
//! so nothing has to be installed on the build machine.

const PROTOS: &[&str] = &[
    "proto/ferrumlog/v1/types.proto",
    "proto/ferrumlog/v1/raft.proto",
    "proto/ferrumlog/v1/log.proto",
    "proto/ferrumlog/v1/admin.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=PROTOC");
    for proto in PROTOS {
        println!("cargo:rerun-if-changed={proto}");
    }

    if std::env::var_os("PROTOC").is_none() {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(PROTOS, &["proto"])?;
    Ok(())
}
