"""Message counts on BOTH branches: raw JSON topic and the Avro topic."""
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

PREFIX = os.environ.get("FS_PREFIX", "raw.fortisiem_cmdb_catalog")
ENTITIES = (sys.argv[1:] or ["case", "report", "monitor", "task", "rule", "user",
                             "watchlist", "lookup_table"])
# candidate Avro topic names — the app derives this, so try the known variants
SUFFIXES = [".avro", "__avro", "-avro"]


async def count(base, u, p, topic):
    try:
        c = await _kafbat_topic_message_count(base, u, p, topic)
    except Exception as e:
        return None, str(e)[:60]
    return (c.get("total_messages") if c.get("ok") else None), c.get("error_code")


async def main():
    base = os.environ.get("KAFBAT_URL", "")
    u = os.environ.get("KAFBAT_USERNAME") or None
    p = os.environ.get("KAFBAT_PASSWORD") or None
    print("%-16s %-10s %-10s %s" % ("entity", "raw", "avro", "avro topic"))
    for e in ENTITIES:
        raw_t = f"{PREFIX}.{e}"
        n_raw, _ = await count(base, u, p, raw_t)
        found = ("-", "none found")
        for s in SUFFIXES:
            n, err = await count(base, u, p, raw_t + s)
            if n is not None:
                found = (n, raw_t + s)
                break
        print("%-16s %-10s %-10s %s" % (e, n_raw, found[0], found[1]))

    for e in ENTITIES[:2]:
        for s in SUFFIXES:
            t = f"{PREFIX}.{e}{s}"
            r = await _kafbat_recent_topic_messages(base, u, p, t, limit=1)
            if r.get("ok") and r.get("messages"):
                print("\nAVRO sample from %s:\n   %s" % (t, str(r["messages"][0].get("value"))[:400]))
                break


asyncio.run(main())
