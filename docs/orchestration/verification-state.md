# Verification State

Records actual evidence for anything marked VERIFIED. No entry = not verified.

| Item | Evidence | Date |
|---|---|---|
| Infra reachability | NiFi 2.9.0 about OK (JWT auth); Apicurio 3.2.1 (v2/v3/ccompat v7 subjects listed); Kafka Connect 4.2.0 (plugins: OpenSearchSink, IcebergSink); APISIX admin routes listed (10 routes, fortisiem/rapid7 upstreams); Kafka TCP 9092 FAIL from dev machine (Test-NetConnection False) | 2026-08-13 |
| Dedup reference flow | `DummyJson_Dedup` PG exported from live NiFi → reference/nifi-flows/DummyJson_Dedup.json; pattern documented in analysis/dedup-reference-flow.md (Groovy SHA-256 hash + DetectDuplicate + RedisDistributedMapCacheClientService TTL 24h + RedisConnectionPoolService redis:6379) | 2026-08-13 |
| Demo_flows contain no dedup | grep across 5 exports: no DetectDuplicate/Redis/hash processors (analysis/nifi-reference-flows.md §6) | 2026-08-13 |
