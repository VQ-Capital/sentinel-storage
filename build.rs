// ========== DOSYA: sentinel-storage/build.rs ==========
use std::io::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=proto/market_data.proto");
    prost_build::compile_protos(&["proto/market_data.proto"], &["proto/"])?;
    Ok(())
}