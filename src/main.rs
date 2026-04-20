use anyhow::{Context, Result};
use futures_util::StreamExt;
use prost::Message;
use qdrant_client::qdrant::{
    CreateCollectionBuilder, Distance, PointStruct, UpsertPointsBuilder, VectorParamsBuilder,
};
use qdrant_client::Qdrant;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};
use uuid::Uuid;

pub mod sentinel_protos {
    pub mod market {
        include!(concat!(env!("OUT_DIR"), "/sentinel.market.v1.rs"));
    }
    pub mod execution {
        include!(concat!(env!("OUT_DIR"), "/sentinel.execution.v1.rs"));
    }
}
use sentinel_protos::execution::ExecutionReport;
use sentinel_protos::market::{AggTrade, MarketStateVector};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let questdb_url = std::env::var("QUESTDB_URL").unwrap_or_else(|_| "127.0.0.1:9009".to_string());

    let nats_client = async_nats::connect(&nats_url)
        .await
        .context("NATS bağlantı hatası")?;

    let qdrant_url =
        std::env::var("QDRANT_URL").unwrap_or_else(|_| "http://localhost:6334".to_string());
    let mut qdrant_client = None;
    for i in 1..=10 {
        if let Ok(client) = Qdrant::from_url(&qdrant_url).build() {
            if client.health_check().await.is_ok() {
                qdrant_client = Some(client);
                break;
            }
        }
        warn!("⚠️ Qdrant hazır değil, bekleniyor... ({}/10)", i);
        sleep(Duration::from_secs(2)).await;
    }
    let qdrant_client = qdrant_client.expect("❌ Qdrant'a bağlanılamadı!");

    if !qdrant_client.collection_exists("market_states").await? {
        qdrant_client
            .create_collection(
                CreateCollectionBuilder::new("market_states")
                    .vectors_config(VectorParamsBuilder::new(3, Distance::Cosine)),
            )
            .await?;
    }

    // 1. HAM VERİ YAZICISI
    let nats_clone_1 = nats_client.clone();
    let qdb_url_1 = questdb_url.clone();
    tokio::spawn(async move {
        let mut sub = nats_clone_1.subscribe("market.trade.>").await.unwrap();
        let mut stream = TcpStream::connect(&qdb_url_1).await.unwrap();
        while let Some(msg) = sub.next().await {
            if let Ok(trade) = AggTrade::decode(msg.payload) {
                let line = format!(
                    "trades,symbol={} price={},quantity={},buyer_maker={} {}\n",
                    trade.symbol,
                    trade.price,
                    trade.quantity,
                    trade.is_buyer_maker,
                    trade.timestamp * 1_000_000
                );
                let _ = stream.write_all(line.as_bytes()).await;
            }
        }
    });

    // 2. VEKTÖR VE AI DUYGU YAZICISI (Hem Qdrant Hem QuestDB'ye yazar)
    let nats_clone_2 = nats_client.clone();
    let qdb_url_2 = questdb_url.clone(); // YENİ: QuestDB bağlantısı
    tokio::spawn(async move {
        let mut sub = nats_clone_2.subscribe("state.vector.>").await.unwrap();
        let mut stream = TcpStream::connect(&qdb_url_2).await.unwrap(); // YENİ: TCP Akışı
        while let Some(msg) = sub.next().await {
            if let Ok(state) = MarketStateVector::decode(msg.payload) {
                // A) Qdrant'a Vektör Gönder
                let point = PointStruct::new(
                    Uuid::new_v4().to_string(),
                    vec![
                        state.price_velocity as f32,
                        state.volume_imbalance as f32,
                        state.sentiment_score as f32,
                    ],
                    [
                        ("symbol", state.symbol.clone().into()),
                        ("velocity", state.price_velocity.into()),
                    ],
                );
                let _ = qdrant_client
                    .upsert_points(UpsertPointsBuilder::new("market_states", vec![point]))
                    .await;

                // B) QuestDB'ye AI Skorunu Gönder (GRAFANA RADARI İÇİN EKLENDİ)
                let line = format!(
                    "market_states,symbol={} velocity={},imbalance={},sentiment_score={} {}\n",
                    state.symbol,
                    state.price_velocity,
                    state.volume_imbalance,
                    state.sentiment_score,
                    chrono::Utc::now().timestamp_nanos_opt().unwrap()
                );
                let _ = stream.write_all(line.as_bytes()).await;
            }
        }
    });

    // 3. PAPER TRADE YAZICISI
    let mut sub = nats_client.subscribe("execution.report.>").await?;
    let mut stream = TcpStream::connect(&questdb_url).await?;
    info!("💾 Paralel Storage Motoru devrede.");

    while let Some(msg) = sub.next().await {
        if let Ok(rep) = ExecutionReport::decode(msg.payload) {
            let line = format!(
                "paper_trades,symbol={},side={} exec_price={},qty={},pnl={},commission={} {}\n",
                rep.symbol,
                rep.side,
                rep.execution_price,
                rep.quantity,
                rep.realized_pnl,
                rep.commission,
                chrono::Utc::now().timestamp_nanos_opt().unwrap()
            );
            let _ = stream.write_all(line.as_bytes()).await;
        }
    }
    Ok(())
}
