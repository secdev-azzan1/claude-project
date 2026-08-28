import asyncio
import sys
import json
import time

sys.path.insert(0, "backend")

import db as dbmod
from services.adapter.deployer import nifi_apply
from services.adapter.deployer.lifecycle import _load_connections, _active_connection, _nifi_conn_dict


async def main():
    flow_id = sys.argv[1]
    trigger_id = sys.argv[2]
    wait_seconds = int(sys.argv[3]) if len(sys.argv) > 3 else 20

    await dbmod.init_db()
    db = dbmod.get_db()
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    if not nifi_conn_doc:
        print("NO ACTIVE NIFI CONNECTION")
        return
    conn_dict = _nifi_conn_dict(nifi_conn_doc)

    print(f"[{flow_id}] firing trigger {trigger_id} RUN_ONCE")
    result = await nifi_apply._set_processors_state(conn_dict, [trigger_id], "RUN_ONCE")
    print("RUN_ONCE result:", json.dumps(result))

    print(f"[{flow_id}] waiting {wait_seconds}s for cascade")
    await asyncio.sleep(wait_seconds)
    print(f"[{flow_id}] done waiting")


asyncio.run(main())
