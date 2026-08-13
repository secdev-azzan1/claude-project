"""Live integration tests against real, externally-configured infrastructure
(NiFi, Kafka Connect, Kafka — see backend/.env). Marked `@pytest.mark.live`
and excluded by default (see backend/pytest.ini); run explicitly with
`-m live`."""
