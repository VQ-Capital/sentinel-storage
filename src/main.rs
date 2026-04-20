// ========== DOSYA: sentinel-storage/src/main.rs ==========
use anyhow::{Context, Result};
use async_nats::Client;
use futures_util::StreamExt;
use prost::Message;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::{error, info};
use tracing_subscriber;

pub mod sentinel_market {
    include!(concat!(env!("OUT_DIR"), "/sentinel.market.rs"));
}
use sentinel_market::AggTrade;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("💾 Sentinel-Storage başlatılıyor...");

    // 1. NATS'A BAĞLAN
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let nats_client = async_nats::connect(&nats_url).await.context("NATS bağlantı hatası")?;
    info!("✅ NATS Omurgasına bağlanıldı.");

    // 2. QUESTDB'YE BAĞLAN (ILP Portu - TCP 9009)
    let questdb_url = std::env::var("QUESTDB_URL").unwrap_or_else(|_| "127.0.0.1:9009".to_string());
    let mut questdb_stream = TcpStream::connect(&questdb_url).await.context("QuestDB TCP bağlantı hatası")?;
    info!("✅ QuestDB ILP portuna bağlanıldı: {}", questdb_url);

    // 3. NATS KANALINA ABONE OL (Tüm trade verilerini dinler)
    let mut subscriber = nats_client.subscribe("market.trade.>").await?;
    info!("🎧 'market.trade.>' kanalındaki veriler dinleniyor ve QuestDB'ye yazılıyor...");

    // 4. DİNLEME VE YAZMA DÖNGÜSÜ
    while let Some(message) = subscriber.next().await {
        // Gelen baytları Protobuf kullanarak asıl veriye (Struct) çevir
        match AggTrade::decode(message.payload.clone()) {
            Ok(trade) => {
                // QuestDB InfluxDB Line Protocol Formatı:
                // tablo_adi,tag1=değer,tag2=değer field1=değer,field2=değer timestamp
                
                // Binance timestamp milisaniye, QuestDB nanosaniye ister. O yüzden * 1_000_000
                let timestamp_ns = trade.timestamp * 1_000_000;
                
                let line = format!(
                    "trades,symbol={} price={},quantity={},is_buyer_maker={} {}\n",
                    trade.symbol,
                    trade.price,
                    trade.quantity,
                    trade.is_buyer_maker,
                    timestamp_ns
                );

                // QuestDB'ye TCP üzerinden fırlat
                if let Err(e) = questdb_stream.write_all(line.as_bytes()).await {
                    error!("QuestDB Yazma Hatası: {:?}", e);
                    // Bağlantı koparsa yeniden bağlanma mantığı eklenebilir
                }
            }
            Err(e) => error!("Protobuf Decode Hatası: {:?}", e),
        }
    }

    Ok(())
}