// ========== DOSYA: sentinel-storage/src/main.rs ==========
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
use tracing::{error, info, warn};
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
use sentinel_protos::market::{AggTrade, MarketStateVector, RawNewsEvent};

async fn connect_questdb_with_retry(url: &str) -> TcpStream {
    loop {
        match TcpStream::connect(url).await {
            Ok(stream) => {
                info!("✅ QuestDB bağlantısı sağlandı: {}", url);
                return stream;
            }
            Err(e) => {
                warn!(
                    "⚠️ QuestDB bağlantı hatası, 3 saniye sonra deneniyor... Hata: {}",
                    e
                );
                sleep(Duration::from_secs(3)).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let questdb_url = std::env::var("QUESTDB_URL").unwrap_or_else(|_| "127.0.0.1:9009".to_string());
    let qdrant_url =
        std::env::var("QDRANT_URL").unwrap_or_else(|_| "http://localhost:6334".to_string());

    let nats_client = async_nats::connect(&nats_url)
        .await
        .context("NATS bağlantı hatası")?;

    let mut qdrant_client = None;
    for i in 1..=10 {
        if let Ok(client) = Qdrant::from_url(&qdrant_url).build() {
            if client.health_check().await.is_ok() {
                qdrant_client = Some(client);
                info!("✅ Qdrant bağlandı.");
                break;
            }
        }
        warn!("⚠️ Qdrant hazır değil, bekleniyor... ({}/10)", i);
        sleep(Duration::from_secs(2)).await;
    }
    let qdrant_client = qdrant_client.context("❌ Qdrant'a bağlanılamadı!")?;

    if !qdrant_client.collection_exists("market_states").await? {
        qdrant_client
            .create_collection(
                CreateCollectionBuilder::new("market_states")
                    .vectors_config(VectorParamsBuilder::new(3, Distance::Cosine)),
            )
            .await?;
    }

    info!("💾 Paralel Storage Motoru (Zero-Tolerance + NLP Memory) devrede.");

    // 1. DİNLEYİCİ: HAM PİYASA VERİLERİ (Trades)
    let nats_c1 = nats_client.clone();
    let qdb_u1 = questdb_url.clone();
    tokio::spawn(async move {
        let mut sub = match nats_c1.subscribe("market.trade.>").await {
            Ok(s) => s,
            Err(e) => {
                error!("Ham veri NATS aboneliği başarısız: {}", e);
                return;
            }
        };
        let mut stream = connect_questdb_with_retry(&qdb_u1).await;

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
                if stream.write_all(line.as_bytes()).await.is_err() {
                    warn!("QuestDB Yazma Hatası (Ham Veri)! Yeniden bağlanılıyor...");
                    stream = connect_questdb_with_retry(&qdb_u1).await;
                }
            }
        }
    });

    // 2. DİNLEYİCİ: PİYASA DURUM VEKTÖRLERİ (Market States)
    let nats_c2 = nats_client.clone();
    let qdb_u2 = questdb_url.clone();
    let qdrant_c2 = qdrant_client.clone();
    tokio::spawn(async move {
        let mut sub = match nats_c2.subscribe("state.vector.>").await {
            Ok(s) => s,
            Err(e) => {
                error!("Vektör aboneliği başarısız: {}", e);
                return;
            }
        };
        let mut stream = connect_questdb_with_retry(&qdb_u2).await;

        while let Some(msg) = sub.next().await {
            if let Ok(state) = MarketStateVector::decode(msg.payload) {
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
                        ("timestamp", (state.window_end_time).into()),
                    ],
                );
                let _ = qdrant_c2
                    .upsert_points(UpsertPointsBuilder::new("market_states", vec![point]))
                    .await;

                let line = format!(
                    "market_states,symbol={} velocity={},imbalance={},sentiment_score={} {}\n",
                    state.symbol,
                    state.price_velocity,
                    state.volume_imbalance,
                    state.sentiment_score,
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                );
                if stream.write_all(line.as_bytes()).await.is_err() {
                    warn!("QuestDB Yazma Hatası (Market State)! Yeniden bağlanılıyor...");
                    stream = connect_questdb_with_retry(&qdb_u2).await;
                }
            }
        }
    });

    // 3. DİNLEYİCİ: İŞLEM RAPORLARI (Execution Reports)
    let nats_c3 = nats_client.clone();
    let qdb_u3 = questdb_url.clone();
    tokio::spawn(async move {
        let mut sub = match nats_c3.subscribe("execution.report.>").await {
            Ok(s) => s,
            Err(e) => {
                error!("Execution rapor aboneliği başarısız: {}", e);
                return;
            }
        };
        let mut stream = connect_questdb_with_retry(&qdb_u3).await;

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
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                );
                if stream.write_all(line.as_bytes()).await.is_err() {
                    warn!("QuestDB Yazma Hatası (Execution)! Yeniden bağlanılıyor...");
                    stream = connect_questdb_with_retry(&qdb_u3).await;
                }
            }
        }
    });

    // 4. DİNLEYİCİ: HAM HABER AKIŞI (Raw News)
    let nats_c4 = nats_client.clone();
    let qdb_u4 = questdb_url.clone();
    tokio::spawn(async move {
        let mut sub = match nats_c4.subscribe("news.raw.>").await {
            Ok(s) => s,
            Err(e) => {
                error!("Ham haber NATS aboneliği başarısız: {}", e);
                return;
            }
        };
        let mut stream = connect_questdb_with_retry(&qdb_u4).await;

        while let Some(msg) = sub.next().await {
            if let Ok(news) = RawNewsEvent::decode(msg.payload) {
                // ILP Format kısıtlamaları: Boşluklu String'ler çift tırnakla sarılmalı ve içindeki tırnaklar kaçışlanmalıdır.
                let safe_headline = news.headline.replace('\"', "\\\"");
                let line = format!(
                    "raw_news,source={} headline=\"{}\" {}\n",
                    news.source,
                    safe_headline,
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                );
                if stream.write_all(line.as_bytes()).await.is_err() {
                    warn!("QuestDB Yazma Hatası (Raw News)! Yeniden bağlanılıyor...");
                    stream = connect_questdb_with_retry(&qdb_u4).await;
                }
            }
        }
    });

    // 5. DİNLEYİCİ: SEMANTİK VEKTÖRLER (NLP Sentiment)
    let nats_c5 = nats_client.clone();
    let qdb_u5 = questdb_url.clone();
    tokio::spawn(async move {
        let mut sub = match nats_c5.subscribe("intelligence.news.vector").await {
            Ok(s) => s,
            Err(e) => {
                error!("Semantik vektör NATS aboneliği başarısız: {}", e);
                return;
            }
        };
        let mut stream = connect_questdb_with_retry(&qdb_u5).await;

        while let Some(msg) = sub.next().await {
            if let Ok(vec) = SemanticVector::decode(msg.payload) {
                let safe_headline = vec.original_headline.replace('\"', "\\\"");
                let line = format!(
                    "semantic_vectors,symbol={},source={} sentiment_score={},headline=\"{}\" {}\n",
                    vec.symbol,
                    vec.source,
                    vec.sentiment_score,
                    safe_headline,
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                );
                if stream.write_all(line.as_bytes()).await.is_err() {
                    warn!("QuestDB Yazma Hatası (Semantic Vector)! Yeniden bağlanılıyor...");
                    stream = connect_questdb_with_retry(&qdb_u5).await;
                }
            }
        }
    });

    // Ana thread'i asılı tut
    tokio::signal::ctrl_c()
        .await
        .context("Sinyal yakalanamadı")?;
    Ok(())
}
