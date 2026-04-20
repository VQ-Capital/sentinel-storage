// ========== DOSYA: sentinel-storage/src/main.rs ==========
use anyhow::{Context, Result};
use futures_util::StreamExt;
use prost::Message;
use qdrant_client::qdrant::{CreateCollectionBuilder, Distance, PointStruct, VectorParamsBuilder, UpsertPointsBuilder};
use qdrant_client::Qdrant;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::{error, info};
use uuid::Uuid;

pub mod sentinel_market {
    include!(concat!(env!("OUT_DIR"), "/sentinel.market.rs"));
}
use sentinel_market::{AggTrade, MarketStateVector};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("💾 Sentinel-Storage (Multi-DB) başlatılıyor...");

    // 1. NATS BAĞLANTISI
    let nats_url = "nats://localhost:4222";
    let nats_client = async_nats::connect(nats_url)
        .await
        .context("NATS bağlantı hatası")?;
    info!("✅ NATS bağlandı.");

    // 2. QUESTDB BAĞLANTISI (TCP ILP)
    let questdb_url = "127.0.0.1:9009";
    let mut questdb_stream = TcpStream::connect(questdb_url).await.context("QuestDB TCP bağlantı hatası")?;
    info!("✅ QuestDB bağlandı.");

    // 3. QDRANT BAĞLANTISI
    let qdrant_client = Qdrant::from_url("http://localhost:6334").build()?;
    let collection_name = "market_states";

    // Koleksiyon kontrolü ve oluşturma
    if !qdrant_client.collection_exists(collection_name).await? {
        qdrant_client
            .create_collection(
                CreateCollectionBuilder::new(collection_name)
                    .vectors_config(VectorParamsBuilder::new(2, Distance::Cosine))
            )
            .await?;
        info!("💎 Qdrant Koleksiyonu oluşturuldu: {}", collection_name);
    }

    // 4. KANALLARI DİNLE
    let mut state_sub = nats_client.subscribe("state.vector.>").await?;
    let mut trade_sub = nats_client.subscribe("market.trade.>").await?;

    info!("🎧 Kanallar dinleniyor...");

    loop {
        tokio::select! {
            // A. TRADE -> QUESTDB
            Some(msg) = trade_sub.next() => {
                if let Ok(trade) = AggTrade::decode(msg.payload) {
                    let line = format!(
                        "trades,symbol={} price={},quantity={},buyer_maker={} {}\n",
                        trade.symbol, trade.price, trade.quantity, trade.is_buyer_maker, trade.timestamp * 1_000_000
                    );
                    let _ = questdb_stream.write_all(line.as_bytes()).await;
                }
            }

            // B. VECTOR -> QDRANT
            Some(msg) = state_sub.next() => {
                if let Ok(state) = MarketStateVector::decode(msg.payload) {
                    // f64 -> f32 dönüşümü
                    let vector: Vec<f32> = state.embeddings.iter().map(|&x| x as f32).collect();
                    
                    // Sembol bilgisini loglarda kullanmak için kopyalıyoruz
                    let symbol_for_log = state.symbol.clone();

                    // Point oluşturma
                    let point = PointStruct::new(
                        Uuid::new_v4().to_string(),
                        vector,
                        [
                            ("symbol", state.symbol.into()), // Burada sahiplik qdrant'a geçer
                            ("velocity", state.price_velocity.into()),
                            ("imbalance", state.volume_imbalance.into()),
                        ]
                    );

                    let upsert_request = UpsertPointsBuilder::new(collection_name, vec![point]);
                    
                    if let Err(e) = qdrant_client.upsert_points(upsert_request).await {
                        error!("Qdrant Yazma Hatası: {:?}", e);
                    } else {
                        info!("💎 [QDRANT] Vektör mühürlendi: {}", symbol_for_log);
                    }
                }
            }
        }
    }
}