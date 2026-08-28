import asyncio
import sys
import json

sys.path.insert(0, ".")

import db as dbmod


async def main():
    await dbmod.init_db()
    db = dbmod.get_db()
    flow = await db["flows_v2"].find_one({"id": "flow-tags"})
    for b in flow["blocks"]:
        if b["id"] in ("b-tag-site", "b-tag-site-write", "b-tag-asset", "b-tag-asset-write"):
            print(json.dumps(b, indent=2))
            print("====")


asyncio.run(main())
