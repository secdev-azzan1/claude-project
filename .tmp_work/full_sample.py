import asyncio, sys, json
sys.path.insert(0, ".")
import db as dbmod
from services.adapter.deployer.lifecycle import _load_connections, _active_connection
from services import kafka_client

async def main():
    topic = sys.argv[1]
    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    kafka_conn = _active_connection(connections, "kafka")
    cfg = kafka_conn.config
    res = await kafka_client._kafbat_recent_topic_messages(cfg["proxyUrl"], cfg.get("kafbatUsername"), cfg.get("kafbatPassword"), topic, 1)
    v = json.loads(res["messages"][0]["value"])
    print(json.dumps(v, indent=2))

asyncio.run(main())
