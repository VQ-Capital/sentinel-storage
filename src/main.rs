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
use chrono;

pub mod sentinel_protos {
    pub mod market {
        include!(concat!(env!("OUT_DIR"), "/sentinel.market.rs"));
    }
    pub mod execution {
        include!(concat!(env!("OUT_DIR"), "/sentinel.execution.rs"));
    }
}

use sentinel_protos::market::{AggTrade, MarketStateVector};
use sentinel_protos::execution::TradeSignal;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("💾 Sentinel-Storage (Full-DB) başlatılıyor...");

    // 1. BAĞLANTILAR
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let nats_client = async_nats::connect(&nats_url).await.context("NATS bağlantı hatası")?;

    let questdb_url = std::env::var("QUESTDB_URL").unwrap_or_else(|_| "127.0.0.1:9009".to_string());
    let mut questdb_stream = TcpStream::connect(&questdb_url).await.context("QuestDB TCP bağlantı hatası")?;

    let qdrant_url = std::env::var("QDRANT_URL").unwrap_or_else(|_| "http://localhost:6334".to_string());
    let qdrant_client = Qdrant::from_url(&qdrant_url).build()?;
    let collection_name = "market_states";

    // 2. QDRANT INIT
    if !qdrant_client.collection_exists(collection_name).await? {
        qdrant_client.create_collection(
            CreateCollectionBuilder::new(collection_name)
                .vectors_config(VectorParamsBuilder::new(3, Distance::Cosine)) // 3D: V, I, S
        ).await?;
        info!("💎 Qdrant Koleksiyonu oluşturuldu: {}", collection_name);
    }

    // 3. TÜM KANALLARA ABONE OL
    let mut trade_sub = nats_client.subscribe("market.trade.>").await?;
    let mut state_sub = nats_client.subscribe("state.vector.>").await?;
    let mut signal_sub = nats_client.subscribe("signal.trade.>").await?;

    info!("🎧 Ingest, Inference ve Signal kanalları dinleniyor...");

    loop {
        tokio::select! {
            // A. HAM VERİ -> QUESTDB
            Some(msg) = trade_sub.next() => {
                if let Ok(trade) = AggTrade::decode(msg.payload) {
                    let line = format!(
                        "trades,symbol={} price={},quantity={},buyer_maker={} {}\n",
                        trade.symbol, trade.price, trade.quantity, trade.is_buyer_maker, trade.timestamp * 1_000_000
                    );
                    let _ = questdb_stream.write_all(line.as_bytes()).await;
                }
            }

            // B. DURUM VEKTÖRÜ -> QDRANT
            Some(msg) = state_sub.next() => {
                if let Ok(state) = MarketStateVector::decode(msg.payload) {
                    let vector: Vec<f32> = state.embeddings.iter().map(|&x| x as f32).collect();
                    let point = PointStruct::new(
                        Uuid::new_v4().to_string(),
                        vector,
                        [
                            ("symbol", state.symbol.clone().into()),
                            ("velocity", state.price_velocity.into()),
                            ("sentiment", state.sentiment_score.into()),
                        ]
                    );
                    let _ = qdrant_client.upsert_points(UpsertPointsBuilder::new(collection_name, vec![point])).await;
                }
            }

            // C. SİNYAL -> QUESTDB (Grafana için)
            Some(msg) = signal_sub.next() => {
                if let Ok(sig) = TradeSignal::decode(msg.payload) {
                    let line = format!(
                        "signals,symbol={} confidence_score={},type={} reason=\"{}\" {}\n",
                        sig.symbol, sig.confidence_score, sig.r#type, sig.reason, chrono::Utc::now().timestamp_nanos_opt().unwrap()
                    );
                    let _ = questdb_stream.write_all(line.as_bytes()).await;
                    info!("🎯 Sinyal arşive kaydedildi: {} -> {}", sig.symbol, sig.r#type);
                }
            }
        }
    }
}