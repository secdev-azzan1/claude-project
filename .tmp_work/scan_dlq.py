import asyncio
import sys
import json

sys.path.insert(0, ".")

import db as dbmod
from services.adapter.deployer.lifecycle import _load_connections, _active_connection
from services import kafka_client


async def main():
    topic = sys.argv[1]
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 100

    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    kafka_conn = _active_connection(connections, "kafka")
    cfg = kafka_conn.config
    proxy_url = cfg["proxyUrl"]
    username = cfg.get("kafbatUsername")
    password = cfg.get("kafbatPassword")

    res = await kafka_client._kafbat_recent_topic_messages(proxy_url, username, password, topic, limit)
    if not res.get("ok"):
        print("error:", res)
        return
    msgs = res.get("messages", [])
    print("total fetched:", len(msgs))
    kinds = {}
    scalar_examples = []
    for m in msgs:
        val = m.get("value")
        try:
            parsed = json.loads(val) if isinstance(val, str) else val
        except Exception:
            parsed = val
        t = type(parsed).__name__
        kinds[t] = kinds.get(t, 0) + 1
        if not isinstance(parsed, dict):
            scalar_examples.append(parsed)
        elif "resources" in json.dumps(parsed) and "sites" in json.dumps(parsed):
            pass
    print("kinds:", kinds)
    print("scalar examples (first 10):", scalar_examples[:10])
    # print any dict with a 'links' containing /sites/ AND top-level 'resources' present (meaning it had site data but still got dlq'd)
    for m in msgs:
        val = m.get("value")
        try:
            parsed = json.loads(val) if isinstance(val, str) else val
        except Exception:
            continue
        if isinstance(parsed, dict) and "resources" in parsed:
            print("DLQ record WITH resources key:", json.dumps(parsed)[:300])


asyncio.run(main())
