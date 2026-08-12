"""One-time cleanup: delete source records not referenced by any flow.

A source is owned by its flow; historically deleting a flow left its source behind,
inflating the dashboard's "Total Sources" count. This removes those orphans.
Backs up ALL sources to a JSON file first so the operation is reversible.
"""
import json
import os
from datetime import datetime, timezone

from pymongo import MongoClient

MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "nif_abstractor")

client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
db = client[DB_NAME]

sources = list(db.sources.find({}, {"_id": 0}))
flows = list(db.flows.find({}, {"_id": 0, "source_id": 1}))

referenced = {str(f.get("source_id")) for f in flows if f.get("source_id")}
orphans = [s for s in sources if str(s.get("id")) not in referenced]

stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
backup_path = f"sources_backup_{stamp}.json"
with open(backup_path, "w", encoding="utf-8") as fh:
    json.dump(sources, fh, ensure_ascii=False, default=str, indent=2)

print(f"Backup written: {backup_path} ({len(sources)} sources saved)")
print(f"Total sources:      {len(sources)}")
print(f"Referenced by flow: {len(referenced)}  (kept)")
print(f"Orphaned:           {len(orphans)}  (to delete)")

deleted = 0
for s in orphans:
    res = db.sources.delete_one({"id": s.get("id")})
    deleted += res.deleted_count

remaining = db.sources.count_documents({})
print(f"Deleted:            {deleted}")
print(f"Sources remaining:  {remaining}")
