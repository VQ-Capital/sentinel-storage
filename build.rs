// ========== DOSYA: sentinel-storage/build.rs ==========
fn main() -> std::io::Result<()> {
    // sentinel-spec'den proto/ klasörüne iki dosyayı da kopyaladığından emin ol!
    prost_build::compile_protos(
        &["proto/market_data.proto", "proto/execution.proto"],
        &["proto/"],
    )?;
    Ok(())
}
