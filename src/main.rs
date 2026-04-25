// ========== DOSYA: sentinel-storage/src/main.rs ==========
use anyhow::{Context, Result};
use futures_util::StreamExt;
use prost::Message;
use qdrant_client::qdrant::{
    CreateCollectionBuilder, Distance, PointStruct, UpsertPointsBuilder, VectorParamsBuilder,
};
use qdrant_client::Qdrant;
use std::sync::Arc; // FIX: Arc artık kapsamda
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn}; // FIX: error makrosu kullanıldı
use uuid::Uuid;

pub mod sentinel_protos {
    pub mod market {
        include!(concat!(env!("OUT_DIR"), "/sentinel.market.v1.rs"));
    }
    pub mod execution {
        include!(concat!(env!("OUT_DIR"), "/sentinel.execution.v1.rs"));
    }
    pub mod intelligence {
        include!(concat!(env!("OUT_DIR"), "/sentinel.intelligence.v1.rs"));
    }
}
use sentinel_protos::execution::ExecutionReport;
use sentinel_protos::intelligence::SemanticVector;
use sentinel_protos::market::{AggTrade, MarketStateVector}; // FIX: RawNewsEvent kaldırıldı

// -----------------------------------------------------------------------------
// 🛠️ QUESTDB CONNECTION (Self-Healing)
// -----------------------------------------------------------------------------
async fn connect_questdb(url: &str) -> TcpStream {
    loop {
        match TcpStream::connect(url).await {
            Ok(stream) => {
                info!("✅ QuestDB Link Established: {}", url);
                return stream;
            }
            Err(e) => {
                warn!("⚠️ QuestDB Down ({}), retrying in 3s...", e);
                sleep(Duration::from_secs(3)).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("💾 VQ-Storage v0.3: Initializing Multi-DB Archiver...");

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let questdb_url = std::env::var("QUESTDB_URL").unwrap_or_else(|_| "127.0.0.1:9009".to_string());
    let qdrant_url =
        std::env::var("QDRANT_URL").unwrap_or_else(|_| "http://localhost:6333".to_string());

    let nats_client = async_nats::connect(&nats_url).await.context("NATS Error")?;

    // Qdrant Setup
    let q_client = Qdrant::from_url(&qdrant_url).build()?;
    if !q_client.collection_exists("market_states").await? {
        q_client
            .create_collection(
                CreateCollectionBuilder::new("market_states")
                    .vectors_config(VectorParamsBuilder::new(4, Distance::Cosine)), // 3 -> 4 yapıldı
            )
            .await?;
    }
    let qdrant = Arc::new(q_client);

    // 1. DİNLEYİCİ: HAM İŞLEMLER (Trades)
    let n1 = nats_client.clone();
    let qu1 = questdb_url.clone();
    tokio::spawn(async move {
        match n1.subscribe("market.trade.>").await {
            Ok(mut sub) => {
                let mut stream = connect_questdb(&qu1).await;
                while let Some(msg) = sub.next().await {
                    if let Ok(t) = AggTrade::decode(msg.payload) {
                        let line = format!(
                            "trades,symbol={} price={},qty={} {}\n",
                            t.symbol,
                            t.price,
                            t.quantity,
                            t.timestamp * 1000000
                        );
                        if stream.write_all(line.as_bytes()).await.is_err() {
                            stream = connect_questdb(&qu1).await;
                        }
                    }
                }
            }
            Err(e) => error!("Trade subscription error: {}", e),
        }
    });

    // 2. DİNLEYİCİ: V3 MARKET STATES (Z-Scores & Fusion)
    let n2 = nats_client.clone();
    let qu2 = questdb_url.clone();
    let qd2 = qdrant.clone();
    tokio::spawn(async move {
        match n2.subscribe("state.vector.>").await {
            Ok(mut sub) => {
                let mut stream = connect_questdb(&qu2).await;
                while let Some(msg) = sub.next().await {
                    if let Ok(s) = MarketStateVector::decode(msg.payload) {
                        // Qdrant Upsert
                        let point = PointStruct::new(
                            Uuid::new_v4().to_string(),
                            s.embeddings.iter().map(|&x| x as f32).collect::<Vec<f32>>(),
                            [
                                ("symbol", s.symbol.clone().into()),
                                ("timestamp", s.window_end_time.into()),
                            ],
                        );
                        let _ = qd2
                            .upsert_points(UpsertPointsBuilder::new("market_states", vec![point]))
                            .await;

                        // QuestDB Write
                        let line = format!("market_states,symbol={} z_velocity={},z_imbalance={},z_sentiment={},z_urgency={} {}\n",
                            s.symbol, s.embeddings[0], s.embeddings[1], s.embeddings[2], s.embeddings[3], s.window_end_time * 1000000);
                        if stream.write_all(line.as_bytes()).await.is_err() {
                            stream = connect_questdb(&qu2).await;
                        }
                    }
                }
            }
            Err(e) => error!("Vector subscription error: {}", e),
        }
    });

    // 3. DİNLEYİCİ: EXECUTION REPORTS (Latency & SLA)
    let n3 = nats_client.clone();
    let qu3 = questdb_url.clone();
    tokio::spawn(async move {
        match n3.subscribe("execution.report.>").await {
            Ok(mut sub) => {
                let mut stream = connect_questdb(&qu3).await;
                while let Some(msg) = sub.next().await {
                    if let Ok(r) = ExecutionReport::decode(msg.payload) {
                        let line = format!("paper_trades,symbol={},side={} exec_price={},qty={},pnl={},latency_ms={} {}\n",
                            r.symbol, r.side, r.execution_price, r.quantity, r.realized_pnl, r.latency_ms, r.timestamp * 1000000);
                        if stream.write_all(line.as_bytes()).await.is_err() {
                            stream = connect_questdb(&qu3).await;
                        }
                    }
                }
            }
            Err(e) => error!("Execution subscription error: {}", e),
        }
    });

    // 4. DİNLEYİCİ: SEMANTİK VEKTÖRLER (NLP)
    let n4 = nats_client.clone();
    let qu4 = questdb_url.clone();
    tokio::spawn(async move {
        match n4.subscribe("intelligence.news.vector").await {
            Ok(mut sub) => {
                let mut stream = connect_questdb(&qu4).await;
                while let Some(msg) = sub.next().await {
                    if let Ok(v) = SemanticVector::decode(msg.payload) {
                        let line = format!(
                            "semantic_vectors,symbol={},source={} score={} {}\n",
                            v.symbol,
                            v.source,
                            v.sentiment_score,
                            v.timestamp * 1000000
                        );
                        if stream.write_all(line.as_bytes()).await.is_err() {
                            stream = connect_questdb(&qu4).await;
                        }
                    }
                }
            }
            Err(e) => error!("Intelligence subscription error: {}", e),
        }
    });

    tokio::signal::ctrl_c().await?;
    info!("🛑 Sentinel Storage shutting down...");
    Ok(())
}
