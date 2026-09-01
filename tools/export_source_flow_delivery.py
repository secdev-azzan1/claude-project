"""Export deployed source flows and their related artifacts into a delivery tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

APP_BASE = "http://127.0.0.1:8010"
FAMILY_PREFIXES = ("Rapid7 Securado", "SentinelOne", "FortiSIEM")


def app_get(path: str) -> Any:
    response = requests.get(f"{APP_BASE}{path}", timeout=60)
    response.raise_for_status()
    return response.json()


def slug(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-.")
    return cleaned or "unnamed"


def family_for(name: str) -> str:
    if name.startswith("Rapid7 Securado"):
        return "Rapid7 Securado"
    if name.startswith("SentinelOne"):
        return "SentinelOne"
    return "FortiSIEM"


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def nifi_session(endpoint: str) -> tuple[requests.Session, dict[str, str]]:
    user = os.environ.get("NIFI_USER")
    password = os.environ.get("NIFI_PASSWORD")
    if not user or not password:
        raise RuntimeError("NIFI_USER and NIFI_PASSWORD are required for NiFi flow downloads.")
    session = requests.Session()
    token_response = session.post(
        f"{endpoint.rstrip('/')}/nifi-api/access/token",
        data={"username": user, "password": password},
        verify=False,
        timeout=60,
    )
    token_response.raise_for_status()
    return session, {"Authorization": f"Bearer {token_response.text.strip()}"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)

    summaries = app_get("/api/v2/flows")
    summaries = summaries if isinstance(summaries, list) else summaries.get("items", [])
    flows = [
        app_get(f"/api/v2/flows/{item['id']}")
        for item in summaries
        if str(item.get("name", "")).startswith(FAMILY_PREFIXES)
    ]
    schemas_payload = app_get("/api/v2/schemas")
    schemas = schemas_payload.get("approved") or []
    syncs = app_get("/api/kafka-connect/syncs")
    connections = app_get("/api/v2/connections")
    connections = connections if isinstance(connections, list) else connections.get("items", [])

    nifi_conn = next((c for c in connections if c.get("type") == "nifi" and c.get("active")), None)
    connect_conn = next((c for c in connections if c.get("type") == "kafka_connect" and c.get("active")), None)
    if not nifi_conn:
        raise RuntimeError("No active NiFi connection exists in the application.")
    nifi_endpoint = (nifi_conn.get("config") or {}).get("url")
    if not nifi_endpoint:
        raise RuntimeError("The active NiFi connection has no URL.")
    session, nifi_headers = nifi_session(nifi_endpoint)
    connect_endpoint = ((connect_conn or {}).get("config") or {}).get("url", "").rstrip("/")

    manifest: dict[str, Any] = {
        "flowCount": len(flows),
        "schemaCount": 0,
        "applicationSyncCount": 0,
        "liveConnectorConfigCount": 0,
        "flows": [],
    }
    missing_schemas: list[dict[str, str]] = []

    for flow in flows:
        if flow.get("state") != "Stopped" or not flow.get("deployedAt") or not flow.get("nifiProcessGroupId"):
            raise RuntimeError(
                f"{flow['name']} is not a complete stopped deployment: "
                f"state={flow.get('state')} deployedAt={flow.get('deployedAt')} pg={flow.get('nifiProcessGroupId')}"
            )
        flow_dir = output / family_for(flow["name"]) / slug(flow["name"])
        write_json(flow_dir / "application" / "flow.json", flow)

        download = session.get(
            f"{nifi_endpoint.rstrip('/')}/nifi-api/process-groups/{flow['nifiProcessGroupId']}/download",
            headers=nifi_headers,
            verify=False,
            timeout=120,
        )
        download.raise_for_status()
        nifi_path = flow_dir / "nifi" / f"{slug(flow['name'])}.json"
        nifi_path.parent.mkdir(parents=True, exist_ok=True)
        nifi_path.write_bytes(download.content)

        flow_schemas = [schema for schema in schemas if schema.get("flowId") == flow["id"]]
        for schema in flow_schemas:
            entity = schema.get("entity") or schema.get("blockId") or schema["id"]
            schema_path = flow_dir / "schemas" / f"{slug(str(entity))}--{slug(schema['id'])}.avsc"
            write_json(schema_path, schema.get("avro") or {})
            write_json(schema_path.with_suffix(".metadata.json"), {k: v for k, v in schema.items() if k != "avro"})
        manifest["schemaCount"] += len(flow_schemas)

        logical_entities = sorted(
            {
                str(block.get("entity"))
                for block in flow.get("blocks") or []
                if block.get("adapter") in {"kafka", "kafka_kc"} and block.get("entity")
            }
        )
        schema_entities = {str(schema.get("entity")) for schema in flow_schemas}
        # Site Assets has a legacy raw writer named "sites" and an Avro entity
        # named "site"; they are the same logical entity for schema coverage.
        if "site" in schema_entities:
            schema_entities.add("sites")
        for entity in sorted(set(logical_entities) - schema_entities):
            marker = {
                "flowId": flow["id"],
                "flowName": flow["name"],
                "entity": entity,
                "reason": "No approved application schema exists; this entity currently has no Avro writer/schema ceremony.",
            }
            write_json(flow_dir / "schemas" / f"{slug(entity)}--NO-APPROVED-SCHEMA.json", marker)
            missing_schemas.append(marker)

        flow_syncs = [sync for sync in syncs if sync.get("linked_flow_id") == flow["id"]]
        for sync in flow_syncs:
            connector_name = sync.get("connector_name") or sync.get("name") or sync["id"]
            sync_dir = flow_dir / "kafka-connect" / slug(str(connector_name))
            write_json(sync_dir / "application-sync.json", sync)
            manifest["applicationSyncCount"] += 1
            live_result: dict[str, Any]
            if connect_endpoint and sync.get("connector_name"):
                live = requests.get(
                    f"{connect_endpoint}/connectors/{quote(str(sync['connector_name']), safe='')}/config",
                    timeout=30,
                    verify=False,
                )
                if live.ok:
                    live_result = {"present": True, "config": live.json()}
                    manifest["liveConnectorConfigCount"] += 1
                else:
                    live_result = {"present": False, "status": live.status_code, "detail": live.text[:1000]}
            else:
                live_result = {"present": False, "detail": "No active Kafka Connect URL or connector name."}
            write_json(sync_dir / "live-connector.json", live_result)

        manifest["flows"].append(
            {
                "id": flow["id"],
                "name": flow["name"],
                "family": family_for(flow["name"]),
                "state": flow["state"],
                "nifiProcessGroupId": flow["nifiProcessGroupId"],
                "entities": logical_entities,
                "approvedSchemaCount": len(flow_schemas),
                "syncCount": len(flow_syncs),
            }
        )

    write_json(output / "manifest.json", manifest)
    write_json(output / "schema-coverage-gaps.json", missing_schemas)
    readme = (
        "# Source flow delivery\n\n"
        "This folder contains stopped NiFi flow-definition downloads, application flow JSON, "
        "approved Avro schemas, and Kafka Connect sink configuration evidence for all in-scope "
        "Rapid7 Securado, SentinelOne, and FortiSIEM flows.\n\n"
        "`manifest.json` is the completeness index. `schema-coverage-gaps.json` records entities "
        "that intentionally have no approved Avro schema because their current flow has no Avro writer.\n"
    )
    (output / "README.md").write_text(readme, encoding="utf-8")

    files = sorted(path for path in output.rglob("*") if path.is_file() and path.name != "checksums.sha256")
    checksum_lines = [f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(output).as_posix()}" for path in files]
    (output / "checksums.sha256").write_text("\n".join(checksum_lines) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
