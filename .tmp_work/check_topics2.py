"""Count messages in the topics the APP actually derived (raw.<flow>.<entity>)."""
import asyncio
import os
import sys

sys.path.insert(0, "backend")
for line in open("backend/.env", encoding="utf-8"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

from services.kafka_client import _kafbat_topic_message_count, _kafbat_recent_topic_messages

PREFIX = sys.argv[1] if len(sys.argv) > 1 else "raw.fortisiem_cmdb_catalog"
ENTITIES = ["case", "report", "monitor", "task", "rule", "user", "watchlist", "lookup_table"]


async def main():
    base = os.environ.get("KAFBAT_URL", "")
    u = os.environ.get("KAFBAT_USERNAME") or None
    p = os.environ.get("KAFBAT_PASSWORD") or None
    total = 0
    for e in ENTITIES:
        topic = f"{PREFIX}.{e}"
        c = await _kafbat_topic_message_count(base, u, p, topic)
        n = c.get("total_messages") if c.get("ok") else c.get("error_code")
        if isinstance(n, int):
            total += n
        print("%-16s %-10s %s" % (e, n, topic))
    print("\nTOTAL:", total)

    for e in ENTITIES:
        r = await _kafbat_recent_topic_messages(base, u, p, f"{PREFIX}.{e}", limit=1)
        if r.get("ok") and r.get("messages"):
            print("\nsample %s:\n   %s" % (e, str(r["messages"][0].get("value"))[:350]))


asyncio.run(main())
