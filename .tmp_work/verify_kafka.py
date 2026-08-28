import asyncio
import sys
import json

sys.path.insert(0, ".")

import db as dbmod
from services.adapter.deployer.lifecycle import _load_connections, _active_connection
from services import kafka_client


async def main():
    topic = sys.argv[1]
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 3

    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    kafka_conn = _active_connection(connections, "kafka")
    cfg = kafka_conn.config
    proxy_url = cfg["proxyUrl"]
    username = cfg.get("kafbatUsername")
    password = cfg.get("kafbatPassword")

    count = await kafka_client._kafbat_topic_message_count(proxy_url, username, password, topic)
    print("message count:", count)

    res = await kafka_client._kafbat_recent_topic_messages(proxy_url, username, password, topic, limit)
    if not res.get("ok"):
        print("messages error:", res)
        return
    for m in res.get("messages", []):
        val = m.get("value")
        try:
            val = json.loads(val) if isinstance(val, str) else val
        except Exception:
            pass
        print(json.dumps(val, indent=2)[:1500])
        print("---")


asyncio.run(main())
