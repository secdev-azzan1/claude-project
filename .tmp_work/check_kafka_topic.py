"""Read the demo topic straight through the backend's own Kafbat client."""
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

TOPIC = sys.argv[1] if len(sys.argv) > 1 else "raw.fortisiem_post_pagination_demo.cmdb_user"


async def main():
    base = os.environ.get("KAFBAT_URL", "")
    user = os.environ.get("KAFBAT_USERNAME") or None
    pwd = os.environ.get("KAFBAT_PASSWORD") or None

    count = await _kafbat_topic_message_count(base, user, pwd, TOPIC)
    print("COUNT:", count)

    msgs = await _kafbat_recent_topic_messages(base, user, pwd, TOPIC, limit=3)
    ok = msgs.get("ok")
    print("MESSAGES ok:", ok)
    if not ok:
        print("  error:", msgs.get("error"), msgs.get("error_code"))
        return
    for m in (msgs.get("messages") or [])[:3]:
        val = m.get("value")
        print("  offset=%s partition=%s" % (m.get("offset"), m.get("partition")))
        print("     value:", (str(val)[:400]))


asyncio.run(main())
