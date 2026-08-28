import asyncio, sys, json
sys.path.insert(0, ".")
import db as dbmod
from services.adapter.deployer.lifecycle import _load_connections, _active_connection
from services import kafka_client

async def main():
    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    kafka_conn = _active_connection(connections, "kafka")
    cfg = kafka_conn.config
    res = await kafka_client._kafbat_recent_topic_messages(cfg["proxyUrl"], cfg.get("kafbatUsername"), cfg.get("kafbatPassword"), "bronze.rapid7_securado.tag_site", 4)
    for m in res["messages"]:
        v = json.loads(m["value"])
        print({k: v.get(k) for k in ["object_id","source_object_id","source_object_type","cursor_window","api_endpoint_export_query_identity"]})

asyncio.run(main())
