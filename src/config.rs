// ========== DOSYA: sentinel-storage/src/config.rs ==========

#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub nats_url: String,
    pub questdb_url: String,
    pub qdrant_url: String,
    pub qdrant_collection: String,
}

impl StorageConfig {
    pub fn from_env() -> Self {
        Self {
            nats_url: std::env::var("NATS_URL")
                .unwrap_or_else(|_| "nats://localhost:4222".to_string()),
            questdb_url: std::env::var("QUESTDB_URL")
                .unwrap_or_else(|_| "127.0.0.1:9009".to_string()),
            qdrant_url: std::env::var("QDRANT_URL")
                .unwrap_or_else(|_| "http://localhost:6333".to_string()),
            qdrant_collection: std::env::var("QDRANT_COLLECTION")
                .unwrap_or_else(|_| "market_states_12d".to_string()),
        }
    }
}
