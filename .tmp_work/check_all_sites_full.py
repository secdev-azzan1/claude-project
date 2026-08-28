import asyncio, sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / "backend" / ".env")

from motor.motor_asyncio import AsyncIOMotorClient
import services.kafka_client as kafka_client
from services.adapter import runtime as runtime_svc

async def main():
    client = AsyncIOMotorClient("mongodb://localhost:27018")
    db = client["dmp_platform"]
    connections = await runtime_svc._load_connections(db)
    kafka_conn_doc = runtime_svc._active_connection(connections, "kafka")
    kafka_conn = runtime_svc._kafka_conn_dict(kafka_conn_doc)

    result = await kafka_client.get_recent_topic_messages(
        bootstrap_servers=kafka_conn["endpoint"],
        topic="raw.rapid7_securado_site_assets.sites",
        limit=500,
        security_protocol=kafka_conn["security_protocol"],
        sasl_mechanism=kafka_conn["sasl_mechanism"],
        sasl_username=kafka_conn["sasl_username"],
        sasl_password=kafka_conn["sasl_password"],
        kafbat_url=kafka_conn["kafbat_url"],
        kafbat_username=kafka_conn["kafbat_username"],
        kafbat_password=kafka_conn["kafbat_password"],
    )
    if not result.get("ok"):
        print("FAILED", result)
        return
    msgs = result.get("messages") or []
    print(f"got {len(msgs)} raw messages")
    rows = []
    for m in msgs:
        try:
            rec = json.loads(m["value"]) if isinstance(m.get("value"), str) else m.get("value")
        except Exception:
            continue
        rows.append((rec.get("id"), rec.get("assets"), repr(rec.get("name"))))
    rows = sorted(set(rows), key=lambda r: (r[0] is None, r[0]))
    for r in rows:
        print(r)
    # highlight top asset counts
    print("\n--- top 10 by assets ---")
    top = sorted([r for r in rows if isinstance(r[1], int)], key=lambda r: -r[1])[:10]
    for r in top:
        print(r)

asyncio.run(main())
