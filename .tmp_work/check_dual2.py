"""Dual-branch check using the topic names the app ACTUALLY assigned.

Adding the kafka_kc block made the app hand the canonical topic name to the Avro
branch and rename the raw JSON branch to `<topic>.variant_1`:
    topic_b-case-k-avro = raw.fortisiem_cmdb_catalog.case
    topic_b-case-k      = raw.fortisiem_cmdb_catalog.case.variant_1
"""
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
ENTITIES = sys.argv[2:] or ["case", "report", "monitor", "task", "rule", "user",
                            "watchlist", "lookup_table"]


async def main():
    base = os.environ.get("KAFBAT_URL", "")
    u = os.environ.get("KAFBAT_USERNAME") or None
    p = os.environ.get("KAFBAT_PASSWORD") or None

    print("%-15s %-12s %-12s" % ("entity", "raw(.variant_1)", "avro(canonical)"))
    tot_r = tot_a = 0
    for e in ENTITIES:
        avro_t = f"{PREFIX}.{e}"
        raw_t = f"{avro_t}.variant_1"
        ca = await _kafbat_topic_message_count(base, u, p, avro_t)
        cr = await _kafbat_topic_message_count(base, u, p, raw_t)
        na = ca.get("total_messages") if ca.get("ok") else ca.get("error_code")
        nr = cr.get("total_messages") if cr.get("ok") else cr.get("error_code")
        if isinstance(na, int):
            tot_a += na
        if isinstance(nr, int):
            tot_r += nr
        print("%-15s %-12s %-12s" % (e, nr, na))
    print("\nTOTAL raw=%s  avro=%s" % (tot_r, tot_a))

    for e in ENTITIES[:3]:
        r = await _kafbat_recent_topic_messages(base, u, p, f"{PREFIX}.{e}.variant_1", limit=1)
        if r.get("ok") and r.get("messages"):
            print("\nRAW sample (%s):\n   %s" % (e, str(r["messages"][0].get("value"))[:400]))
            break


asyncio.run(main())
