import asyncio, sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / "backend" / ".env")

from motor.motor_asyncio import AsyncIOMotorClient
import services.kafka_client as kafka_client
from services.adapter import runtime as runtime_svc

async def check(topic, limit):
    client = AsyncIOMotorClient("mongodb://localhost:27018")
    db = client["dmp_platform"]
    connections = await runtime_svc._load_connections(db)
    kafka_conn_doc = runtime_svc._active_connection(connections, "kafka")
    kafka_conn = runtime_svc._kafka_conn_dict(kafka_conn_doc)

    result = await kafka_client.get_recent_topic_messages(
        bootstrap_servers=kafka_conn["endpoint"],
        topic=topic,
        limit=limit,
        security_protocol=kafka_conn["security_protocol"],
        sasl_mechanism=kafka_conn["sasl_mechanism"],
        sasl_username=kafka_conn["sasl_username"],
        sasl_password=kafka_conn["sasl_password"],
        kafbat_url=kafka_conn["kafbat_url"],
        kafbat_username=kafka_conn["kafbat_username"],
        kafbat_password=kafka_conn["kafbat_password"],
    )
    if not result.get("ok"):
        print(topic, "FAILED", result)
        return
    msgs = result.get("messages") or []
    print(f"{topic}: got {len(msgs)} raw messages")
    hits = []
    site_ids_seen = set()
    for m in msgs:
        try:
            rec = json.loads(m["value"]) if isinstance(m.get("value"), str) else m.get("value")
        except Exception:
            continue
        if rec is None:
            continue
        sid = rec.get("site_id") or rec.get("siteId")
        if sid is not None:
            site_ids_seen.add(sid)
        oid = rec.get("object_id", "")
        name = rec.get("name") or rec.get("host") or ""
        if str(sid) == "40" or ":40_" in str(oid) or ":site:40" in str(oid) or "CCED Windows QUARTER" in json.dumps(rec):
            hits.append(rec)
    print(f"  distinct site_ids seen in sample: {sorted(site_ids_seen, key=lambda x: (str(x)))}")
    print(f"  hits referencing site 40 / CCED Windows QUARTER: {len(hits)}")
    for h in hits[:5]:
        print(json.dumps(h, indent=2)[:1000])

async def main():
    await check("bronze.rapid7_securado.asset", 2000)
    print()
    await check("raw.rapid7_securado_site_assets.asset", 2000)

asyncio.run(main())
