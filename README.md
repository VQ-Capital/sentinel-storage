# 💾 sentinel-memory-archiver (Legacy: sentinel-storage)

**Domain:** Time-Series (QuestDB) & Vector Memory (Qdrant) Writing
**Rol:** Sistemin Hafızası

Sistemin saniyede binlerce mesajı bulan yoğun NATS akışını diske (Storage) asenkron olarak yazar. Olası bağlantı kopukluklarında veritabanı sürücülerini otomatik yeniden bağlar (Auto-Reconnect).

- **Veritabanları:** QuestDB (ILP) ve Qdrant (gRPC)