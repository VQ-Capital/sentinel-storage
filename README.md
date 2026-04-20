# 💾 sentinel-storage (The Memory)

**Sorumluluk:** `NATS` üzerinden akan Protobuf verilerini dinler ve ilgili kalıcı hafızalara (Storage) yazar.
**Veri Yönlendirmesi:**
1. `market.trade.*` kanalını dinler -> QuestDB'ye (Zaman serisi arşiv) yazar.
2. `state.vector.*` kanalını dinler -> Qdrant'a (Vektör veri tabanı) Upsert yapar.
**Dil:** Rust (`async-nats`, `qdrant-client`, `postgres-client`)