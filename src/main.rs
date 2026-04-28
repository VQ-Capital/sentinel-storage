// ========== DOSYA: sentinel-storage/src/main.rs ==========
use anyhow::{Context, Result};
use futures_util::StreamExt;
use prost::Message;
use qdrant_client::qdrant::{
    CreateCollectionBuilder, Distance, PointStruct, UpsertPointsBuilder, VectorParamsBuilder,
};
use qdrant_client::Qdrant;
use std::sync::Arc;
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
    pub mod intelligence {
        include!(concat!(env!("OUT_DIR"), "/sentinel.intelligence.v1.rs"));
    }
    pub mod wallet {
        include!(concat!(env!("OUT_DIR"), "/sentinel.wallet.v1.rs"));
    }
}
use sentinel_protos::execution::ExecutionReport;
use sentinel_protos::intelligence::SemanticVector;
use sentinel_protos::market::{AggTrade, MarketStateVector};
use sentinel_protos::wallet::EquitySnapshot;

async fn connect_questdb(url: &str, label: &str) -> TcpStream {
    loop {
        match TcpStream::connect(url).await {
            Ok(stream) => {
                info!("✅ QuestDB Link Established for [{}] at {}", label, url);
                return stream;
            }
            Err(e) => {
                warn!("⚠️ QuestDB Down for [{}] ({}), retrying...", label, e);
                sleep(Duration::from_secs(3)).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!(
        "📡 Service: {} | Version: 0.5.0 (V5 12D Unified Storage)",
        env!("CARGO_PKG_NAME")
    );

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let questdb_url = std::env::var("QUESTDB_URL").unwrap_or_else(|_| "127.0.0.1:9009".to_string());
    let qdrant_url =
        std::env::var("QDRANT_URL").unwrap_or_else(|_| "http://localhost:6333".to_string());
    let nats_client = async_nats::connect(&nats_url).await.context("NATS Error")?;

    // 🔥 KRİTİK GÜNCELLEME: 12 Boyutlu Koleksiyon İsmi market_states_12d
    let q_client = Qdrant::from_url(&qdrant_url).build()?;
    loop {
        if let Ok(exists) = q_client.collection_exists("market_states_12d").await {
            if !exists {
                let _ = q_client
                    .create_collection(
                        CreateCollectionBuilder::new("market_states_12d")
                            .vectors_config(VectorParamsBuilder::new(12, Distance::Cosine)),
                    )
                    .await;
                info!("💎 Qdrant: 'market_states_12d' collection initialized with 12 dimensions.");
            }
            break;
        }
        warn!("⏳ Qdrant is warming up, retrying...");
        sleep(Duration::from_secs(2)).await;
    }
    let qdrant = Arc::new(q_client);

    // 1. Trades (Ham Borsa Verisi)
    let n1 = nats_client.clone();
    let qu1 = questdb_url.clone();
    tokio::spawn(async move {
        let mut stream = connect_questdb(&qu1, "Trades").await;
        if let Ok(mut sub) = n1.subscribe("market.trade.>").await {
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
                        stream = connect_questdb(&qu1, "Trades").await;
                    }
                }
            }
        }
    });

    // 2. Market States (Vektör Hafıza ve Z-Score Analizi)
    let n2 = nats_client.clone();
    let qu2 = questdb_url.clone();
    let qd2 = qdrant.clone();
    tokio::spawn(async move {
        let mut stream = connect_questdb(&qu2, "MarketStates").await;
        if let Ok(mut sub) = n2.subscribe("state.vector.>").await {
            while let Some(msg) = sub.next().await {
                if let Ok(s) = MarketStateVector::decode(msg.payload) {
                    // Qdrant'a 12 Boyutlu Vektörü ve Meta Verileri Kaydet
                    if s.embeddings.len() == 12 {
                        let point = PointStruct::new(
                            Uuid::new_v4().to_string(),
                            s.embeddings.iter().map(|&x| x as f32).collect::<Vec<f32>>(),
                            [
                                ("symbol", s.symbol.clone().into()),
                                ("timestamp", s.window_end_time.into()),
                                ("velocity", s.price_velocity.into()),
                                ("imbalance", s.volume_imbalance.into()),
                                ("sentiment", s.sentiment_score.into()),
                                ("urgency", s.chain_urgency.into()),
                            ],
                        );
                        let _ = qd2
                            .upsert_points(UpsertPointsBuilder::new(
                                "market_states_12d",
                                vec![point],
                            ))
                            .await;
                    }

                    // QuestDB'ye Z-Score Verilerini Yaz (Grafana Radar İçin)
                    let line = format!("market_states,symbol={} z_velocity={},z_imbalance={},z_sentiment={},z_urgency={} {}\n",
                        s.symbol, s.embeddings[0], s.embeddings[1], s.embeddings[2], s.embeddings[3], s.window_end_time * 1000000);
                    if stream.write_all(line.as_bytes()).await.is_err() {
                        stream = connect_questdb(&qu2, "MarketStates").await;
                    }
                }
            }
        }
    });

    // 3. Execution Reports (Gerçekleşen İşlemler)
    let n3 = nats_client.clone();
    let qu3 = questdb_url.clone();
    tokio::spawn(async move {
        let mut stream = connect_questdb(&qu3, "ExecutionReports").await;
        if let Ok(mut sub) = n3.subscribe("execution.report.>").await {
            while let Some(msg) = sub.next().await {
                if let Ok(r) = ExecutionReport::decode(msg.payload) {
                    let line = format!("paper_trades,symbol={},side={},order_id={} exec_price={},qty={},pnl={},latency_ms={} {}\n",
                        r.symbol, r.side, r.order_id, r.execution_price, r.quantity, r.realized_pnl, r.latency_ms, r.timestamp * 1000000);
                    if stream.write_all(line.as_bytes()).await.is_err() {
                        stream = connect_questdb(&qu3, "ExecutionReports").await;
                    }
                }
            }
        }
    });

    // 4. Semantic Vectors (Haber Analizleri)
    let n4 = nats_client.clone();
    let qu4 = questdb_url.clone();
    tokio::spawn(async move {
        let mut stream = connect_questdb(&qu4, "SemanticVectors").await;
        if let Ok(mut sub) = n4.subscribe("intelligence.news.vector").await {
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
                        stream = connect_questdb(&qu4, "SemanticVectors").await;
                    }
                }
            }
        }
    });

    // 5. Performance (Hazine ve Portföy Durumu)
    let n5 = nats_client.clone();
    let qu5 = questdb_url.clone();
    tokio::spawn(async move {
        let mut stream = connect_questdb(&qu5, "Performance").await;
        if let Ok(mut sub) = n5.subscribe("wallet.equity.snapshot").await {
            while let Some(msg) = sub.next().await {
                if let Ok(e) = EquitySnapshot::decode(msg.payload) {
                    // 🔥 CERRAHİ: sharpe_ratio geri eklendi!
                    let line = format!("performance equity={},balance={},unrealized_pnl={},drawdown_pct={},sharpe_ratio={} {}\n",
                        e.total_equity_usd, e.available_margin_usd, e.total_unrealized_pnl, e.max_drawdown_pct, e.sharpe_ratio, e.timestamp * 1000000);
                    if stream.write_all(line.as_bytes()).await.is_err() {
                        stream = connect_questdb(&qu5, "Performance").await;
                    }
                }
            }
        }
    });

    tokio::signal::ctrl_c().await?;
    info!("🛑 Sentinel Storage shutting down gracefully...");
    Ok(())
}
