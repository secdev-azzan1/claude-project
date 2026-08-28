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
        topic="bronze.rapid7_securado.asset",
        limit=200,
        security_protocol=kafka_conn["security_protocol"],
        sasl_mechanism=kafka_conn["sasl_mechanism"],
        sasl_username=kafka_conn["sasl_username"],
        sasl_password=kafka_conn["sasl_password"],
        kafbat_url=kafka_conn["kafbat_url"],
        kafbat_username=kafka_conn["kafbat_username"],
        kafbat_password=kafka_conn["kafbat_password"],
    )
    print("ok:", result.get("ok"), "error:", result.get("error"), "count:", len(result.get("messages") or []))
    msgs = result.get("messages") or []
    for m in msgs[:2]:
        rec = json.loads(m["value"]) if isinstance(m.get("value"), str) else m.get("value")
        print(json.dumps(rec, indent=2)[:2000])
        print("----")

asyncio.run(main())
