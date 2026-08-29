"""Prove the pagination loop fetched DIFFERENT pages, not the same page N times.

Page size is 50. If pagination advanced, a run of 4 pages yields ~200 distinct
records. If the counters never reached the API (the body-only bug), every page
would be byte-identical and the topic would hold only ~50 distinct records
repeated over and over.
"""
import asyncio
import os
import sys
from collections import Counter

sys.path.insert(0, "backend")

for line in open("backend/.env", encoding="utf-8"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

from services.kafka_client import _kafbat_recent_topic_messages

TOPIC = "raw.fortisiem_post_pagination_demo.cmdb_user"


async def main():
    res = await _kafbat_recent_topic_messages(
        os.environ.get("KAFBAT_URL", ""),
        os.environ.get("KAFBAT_USERNAME") or None,
        os.environ.get("KAFBAT_PASSWORD") or None,
        TOPIC, limit=500,
    )
    if not res.get("ok"):
        print("FAILED:", res)
        return

    msgs = res.get("messages") or []
    names = []
    for m in msgs:
        v = m.get("value")
        if isinstance(v, str):
            import json
            try:
                v = json.loads(v)
            except Exception:
                continue
        if isinstance(v, dict) and "User_Full_Name" in v:
            names.append(v["User_Full_Name"])

    distinct = set(names)
    print("messages sampled :", len(msgs))
    print("records parsed   :", len(names))
    print("DISTINCT names   :", len(distinct))
    print()
    print("Interpretation:")
    print("  page size = 50.")
    print("  <= 50 distinct  => every page was identical (pagination NOT advancing)")
    print("  >  50 distinct  => the loop fetched different pages (pagination WORKING)")
    print()
    dupes = Counter(names).most_common(3)
    print("most repeated names (repeats across the 3 trigger runs are expected):")
    for n, c in dupes:
        print("   %-45r x%d" % (n, c))
    print()
    print("sample of distinct names:")
    for n in sorted(distinct)[:12]:
        print("   ", n)


asyncio.run(main())
