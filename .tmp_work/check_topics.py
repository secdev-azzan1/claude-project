"""Message count for each new FortiSIEM topic, straight through Kafbat."""
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

ENTITIES = sys.argv[1:] or ["case", "report", "monitor", "task", "rule", "user",
                            "watchlist", "lookup_table", "incident"]


async def main():
    base = os.environ.get("KAFBAT_URL", "")
    u = os.environ.get("KAFBAT_USERNAME") or None
    p = os.environ.get("KAFBAT_PASSWORD") or None
    total = 0
    for e in ENTITIES:
        topic = f"bronze.fortisiem.{e}__raw"
        try:
            c = await _kafbat_topic_message_count(base, u, p, topic)
        except Exception as exc:
            print("%-16s ERROR %s" % (e, str(exc)[:80]))
            continue
        if not c.get("ok"):
            print("%-16s %-8s %s" % (e, "-", c.get("error_code")))
            continue
        n = c.get("total_messages", 0)
        total += n
        print("%-16s %-8s %s" % (e, n, topic))
    print("\nTOTAL messages across topics:", total)

    # one sample record from the first topic that has data
    for e in ENTITIES:
        topic = f"bronze.fortisiem.{e}__raw"
        r = await _kafbat_recent_topic_messages(base, u, p, topic, limit=1)
        if r.get("ok") and r.get("messages"):
            print("\nsample from %s:" % topic)
            print("   ", str(r["messages"][0].get("value"))[:400])
            break


asyncio.run(main())
