import asyncio
import sys
from pathlib import Path
from typing import Any, Dict, List

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import nifi_flow_generator as gen


def _fake_nifi_env(monkeypatch):
    call_log: List[Dict[str, Any]] = []
    id_counter = {"n": 0}

    def _next_id() -> str:
        id_counter["n"] += 1
        return f"fake-{id_counter['n']}"

    async def fake_get_root(*a, **kw):
        return "root-pg"

    async def fake_create_pg(nifi_url, parent_pg_id, name, **kw):
        pg_id = _next_id()
        call_log.append({"action": "create_pg", "parent": parent_pg_id, "name": name, "id": pg_id})
        return pg_id

    async def fake_create_processor(nifi_url, pg_id, proc_type, name, **kw):
        proc_id = _next_id()
        call_log.append({
            "action": "create_processor",
            "pg": pg_id,
            "type": proc_type,
            "name": name,
            "id": proc_id,
            "properties": kw.get("properties", {}),
            "auto_terminate": kw.get("auto_terminate", []),
        })
        return proc_id

    async def fake_create_connection(nifi_url, pg_id, src_id, dst_id, rels, **kw):
        conn_id = _next_id()
        call_log.append({"action": "create_connection", "pg": pg_id, "src": src_id, "dst": dst_id, "rels": rels})
        return conn_id

    async def fake_create_cs(nifi_url, pg_id, svc_type, name, properties, **kw):
        return _next_id(), 0

    async def fake_enable_cs(*a, **kw):
        return True

    async def fake_wait_cs(*a, **kw):
        return True

    async def fake_nifi_api_request(nifi_url, method, path, **kw):
        return {"ok": True, "data": {"revision": {"version": 0}}}

    async def fake_trino_column_types(*a, **kw):
        return {}

    monkeypatch.setattr(gen, "get_nifi_root_process_group_id", fake_get_root)
    monkeypatch.setattr(gen, "_create_process_group", fake_create_pg)
    monkeypatch.setattr(gen, "_create_processor", fake_create_processor)
    monkeypatch.setattr(gen, "_create_connection", fake_create_connection)
    monkeypatch.setattr(gen, "_create_controller_service", fake_create_cs)
    monkeypatch.setattr(gen, "_enable_controller_service", fake_enable_cs)
    monkeypatch.setattr(gen, "_wait_cs_enabled", fake_wait_cs)
    monkeypatch.setattr(gen, "nifi_api_request", fake_nifi_api_request)
    monkeypatch.setattr(gen, "_trino_column_types", fake_trino_column_types)
    return call_log


def test_trino_json_object_expr_handles_complex_and_timestamp_types():
    expr = gen._trino_json_object_expr(
        ["accessip", "organization", "ingest_ts"],
        {
            "accessip": "varchar",
            "organization": 'row("attr_id" bigint, "attr_name" varchar, "original_content" varchar)',
            "ingest_ts": "timestamp(6) with time zone",
        },
    )

    assert "'accessip' VALUE \"accessip\"" in expr
    assert "'organization' VALUE json_format(CAST(\"organization\" AS JSON)) FORMAT JSON" in expr
    assert "'ingest_ts' VALUE CAST(\"ingest_ts\" AS varchar)" in expr


def test_trino_source_creates_snapshot_row_publish_flow(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = {
        "name": "asset_history_from_trino",
        "type": "Trino",
        "trino_endpoint": "https://trino.example.com",
        "trino_user": "admin",
        "trino_catalog": "bronze",
        "trino_schema": "rapid7_securado",
        "trino_table": "asset__history",
        "trino_checkpoint_table": "bronze.flow_checkpoint.trino_snapshots",
        "trino_columns": ["id", "object_id", "ip", "hostname"],
        "streams": [
            {
                "id": "asset__history",
                "name": "asset__history",
                "entity": {"kafka": {"topic": "bronze.asset_history.snapshot_rows", "value_format": ""}},
            }
        ],
        "kafka_output": {"topic": "bronze.asset_history.snapshot_rows"},
        "schedule": {"interval_value": 5, "interval_unit": "minutes"},
    }

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source,
        flow_id="flow-trino",
        nifi_conn={"endpoint": "http://nifi:8080", "auth_type": "NONE"},
        kafka_conn={"endpoint": "kafka:9092"},
    ))

    assert result["ok"] is True
    create_pg_calls = [c for c in call_log if c["action"] == "create_pg"]
    flow_pg_id = create_pg_calls[0]["id"]
    trino_pgs = [c for c in create_pg_calls if c["parent"] == flow_pg_id and "trino" in c["name"]]
    assert trino_pgs, f"Expected a Trino child PG, got {create_pg_calls}"

    trino_pg_id = trino_pgs[0]["id"]
    proc_calls = [c for c in call_log if c["action"] == "create_processor" and c["pg"] == trino_pg_id]
    proc_names = {c["name"] for c in proc_calls}
    assert {
        "trigger_snapshot_check",
        "set_trino_headers",
        "write_checkpoint_lookup_sql",
        "submit_checkpoint_lookup_query",
        "write_snapshot_payload_sql",
        "submit_snapshot_payload_query",
        "extract_snapshot_payload",
        "split_snapshot_rows_to_messages",
        "publish_one_row_to_kafka",
        "write_checkpoint_update_sql",
    }.issubset(proc_names)

    split = next(c for c in proc_calls if c["name"] == "split_snapshot_rows_to_messages")
    assert split["type"].endswith(".SplitJson")
    assert split["properties"]["JsonPath Expression"] == "$.rows[*]"

    publish = next(c for c in proc_calls if c["name"] == "publish_one_row_to_kafka")
    assert publish["type"].endswith(".PublishKafka")
    assert publish["properties"]["Topic Name"] == "bronze.asset_history.snapshot_rows"


def test_trino_source_uses_split_checkpoint_fields_and_auto_creates_table(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = {
        "name": "fortisiem_device_from_trino",
        "type": "Trino",
        "trino_endpoint": "https://trino.example.com",
        "trino_user": "admin",
        "trino_catalog": "bronze",
        "trino_schema": "fortisiem",
        "trino_table": "device__history",
        "trino_checkpoint_catalog": "bronze",
        "trino_checkpoint_schema": "checkpoints",
        "trino_checkpoint_table_name": "trino_snapshot_checkpoints",
        "trino_auto_create_checkpoint_table": True,
        "trino_columns": ["accessip", "object_id", "name", "naturalid"],
        "streams": [
            {
                "id": "device__history",
                "name": "device__history",
                "entity": {"kafka": {"topic": "bronze.fortisiem.device_history.rows", "value_format": ""}},
            }
        ],
        "kafka_output": {"topic": "bronze.fortisiem.device_history.rows"},
        "schedule": {"interval_value": 5, "interval_unit": "minutes"},
    }

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source,
        flow_id="flow-trino-fortisiem",
        nifi_conn={"endpoint": "http://nifi:8080", "auth_type": "NONE"},
        kafka_conn={"endpoint": "kafka:9092"},
    ))

    assert result["ok"] is True
    proc_calls = [c for c in call_log if c["action"] == "create_processor"]

    create_sql = next(c for c in proc_calls if c["name"] == "write_checkpoint_create_sql")
    assert 'CREATE TABLE IF NOT EXISTS "bronze"."checkpoints"."trino_snapshot_checkpoints"' in create_sql["properties"]["Replacement Value"]
    assert '"bronze"."fortisiem"."device__history"' not in create_sql["properties"]["Replacement Value"]

    lookup_sql = next(c for c in proc_calls if c["name"] == "write_checkpoint_lookup_sql")
    assert 'FROM "bronze"."checkpoints"."trino_snapshot_checkpoints"' in lookup_sql["properties"]["Replacement Value"]
    assert "bronze.fortisiem.device__history" in lookup_sql["properties"]["Replacement Value"]
    assert 'FROM "bronze"."fortisiem"."device__history"' not in lookup_sql["properties"]["Replacement Value"]

    snapshot_sql = next(c for c in proc_calls if c["name"] == "write_snapshot_payload_sql")
    assert 'FROM "bronze"."fortisiem"."device__history$snapshots"' in snapshot_sql["properties"]["Replacement Value"]
    assert "NULLIF('${checkpoint.snapshot.id}', '')" in snapshot_sql["properties"]["Replacement Value"]
    assert "NULLIF(${checkpoint.snapshot.id}, '')" not in snapshot_sql["properties"]["Replacement Value"]
    assert "ORDER BY committed_at DESC LIMIT 1" in snapshot_sql["properties"]["Replacement Value"]
    assert "CASE WHEN (SELECT checkpoint_id FROM checkpoint) IS NOT NULL THEN committed_at END ASC" not in snapshot_sql["properties"]["Replacement Value"]

    bootstrap_rows_sql = next(c for c in proc_calls if c["name"] == "write_bootstrap_rows_query_sql")
    assert "json_arrayagg" not in bootstrap_rows_sql["properties"]["Replacement Value"]
    assert "json_format(CAST(array_agg(json_parse(row_json)) AS JSON))" in bootstrap_rows_sql["properties"]["Replacement Value"]

    checkpoint_update_sql = next(c for c in proc_calls if c["name"] == "write_checkpoint_update_sql")
    assert "orElse" not in checkpoint_update_sql["properties"]["Replacement Value"]
    assert "${fragment.count}" in checkpoint_update_sql["properties"]["Replacement Value"]

    proc_by_name = {c["name"]: c["id"] for c in proc_calls}
    conn_calls = [c for c in call_log if c["action"] == "create_connection"]
    expected_connections = [
        (
            proc_by_name["submit_checkpoint_create_query"],
            proc_by_name["extract_checkpoint_create_status"],
            ["Response"],
        ),
        (
            proc_by_name["extract_checkpoint_create_status"],
            proc_by_name["route_checkpoint_create_state"],
            ["matched"],
        ),
        (
            proc_by_name["route_checkpoint_create_state"],
            proc_by_name["poll_checkpoint_create_query"],
            ["needs_poll"],
        ),
        (
            proc_by_name["poll_checkpoint_create_query"],
            proc_by_name["extract_checkpoint_create_status"],
            ["Response"],
        ),
        (
            proc_by_name["route_checkpoint_create_state"],
            proc_by_name["write_checkpoint_lookup_sql"],
            ["done"],
        ),
        (
            proc_by_name["submit_checkpoint_update_query"],
            proc_by_name["extract_checkpoint_update_status"],
            ["Response"],
        ),
        (
            proc_by_name["extract_checkpoint_update_status"],
            proc_by_name["route_checkpoint_update_state"],
            ["matched"],
        ),
        (
            proc_by_name["route_checkpoint_update_state"],
            proc_by_name["poll_checkpoint_update_query"],
            ["needs_poll"],
        ),
        (
            proc_by_name["poll_checkpoint_update_query"],
            proc_by_name["extract_checkpoint_update_status"],
            ["Response"],
        ),
    ]
    for src, dst, rels in expected_connections:
        assert {"action": "create_connection", "pg": proc_calls[0]["pg"], "src": src, "dst": dst, "rels": rels} in conn_calls


def test_trino_all_column_mode_builds_row_query_from_information_schema(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = {
        "name": "fortisiem_device_from_trino",
        "type": "Trino",
        "trino_endpoint": "https://trino.example.com",
        "trino_user": "admin",
        "trino_catalog": "bronze",
        "trino_schema": "fortisiem",
        "trino_table": "device__history",
        "trino_checkpoint_catalog": "bronze",
        "trino_checkpoint_schema": "checkpoints",
        "trino_checkpoint_table_name": "trino_snapshot_checkpoints",
        "trino_auto_create_checkpoint_table": True,
        "trino_column_mode": "all",
        "trino_columns": [],
        "streams": [
            {
                "id": "device__history",
                "name": "device__history",
                "entity": {"kafka": {"topic": "bronze.fortisiem.device_history.rows", "value_format": ""}},
            }
        ],
        "kafka_output": {"topic": "bronze.fortisiem.device_history.rows"},
        "schedule": {"interval_value": 5, "interval_unit": "minutes"},
    }

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source,
        flow_id="flow-trino-fortisiem-all-columns",
        nifi_conn={"endpoint": "http://nifi:8080", "auth_type": "NONE"},
        kafka_conn={"endpoint": "kafka:9092"},
    ))

    assert result["ok"] is True
    proc_calls = [c for c in call_log if c["action"] == "create_processor"]
    proc_names = {c["name"] for c in proc_calls}
    assert {
        "write_bootstrap_rows_query_builder_sql",
        "write_incremental_rows_query_builder_sql",
        "submit_rows_query_builder",
        "extract_generated_rows_query",
        "write_generated_rows_query_to_content",
        "submit_rows_query",
        "split_trino_response_rows_to_messages",
        "extract_trino_row_json",
        "write_trino_row_json_to_content",
    }.issubset(proc_names)
    assert "write_rows_payload_to_content" not in proc_names

    bootstrap_builder = next(c for c in proc_calls if c["name"] == "write_bootstrap_rows_query_builder_sql")
    builder_sql = bootstrap_builder["properties"]["Replacement Value"]
    assert 'FROM "bronze".information_schema.columns' in builder_sql
    assert "table_schema = 'fortisiem'" in builder_sql
    assert "table_name = 'device__history'" in builder_sql
    assert "json_object(" in builder_sql
    assert "THEN 'CAST(NULL AS varchar)'" in builder_sql
    assert "AS JSON)) FORMAT JSON" not in builder_sql
    assert "json_format(CAST(array_agg(json_parse(row_json)) AS JSON))" not in builder_sql
    assert "json_parse(row_json)" not in builder_sql

    generated_content = next(c for c in proc_calls if c["name"] == "write_generated_rows_query_to_content")
    assert generated_content["properties"]["Replacement Value"] == "${trino.generated.rows.sql}"
    split_rows = next(c for c in proc_calls if c["name"] == "split_trino_response_rows_to_messages")
    assert split_rows["properties"]["JsonPath Expression"] == "$.data[*]"
    extract_row = next(c for c in proc_calls if c["name"] == "extract_trino_row_json")
    assert extract_row["properties"]["trino.row.json"] == "$[0]"
    row_content = next(c for c in proc_calls if c["name"] == "write_trino_row_json_to_content")
    assert row_content["properties"]["Replacement Value"] == "${trino.row.json}"
    rows_payload_route = next(c for c in proc_calls if c["name"] == "route_rows_payload_state")
    assert rows_payload_route["properties"]["has_payload"] == "${trino.payload:isEmpty():not()}"
    assert rows_payload_route["properties"]["needs_poll"] == "${trino.nextUri:isEmpty():not()}"

    proc_by_name = {c["name"]: c["id"] for c in proc_calls}
    conn_calls = [c for c in call_log if c["action"] == "create_connection"]
    assert {
        "action": "create_connection",
        "pg": proc_calls[0]["pg"],
        "src": proc_by_name["extract_generated_rows_query"],
        "dst": proc_by_name["route_rows_query_builder_state"],
        "rels": ["matched"],
    } in conn_calls
    assert {
        "action": "create_connection",
        "pg": proc_calls[0]["pg"],
        "src": proc_by_name["route_rows_query_builder_state"],
        "dst": proc_by_name["poll_rows_query_builder"],
        "rels": ["needs_poll"],
    } in conn_calls
    assert {
        "action": "create_connection",
        "pg": proc_calls[0]["pg"],
        "src": proc_by_name["route_rows_query_builder_state"],
        "dst": proc_by_name["write_generated_rows_query_to_content"],
        "rels": ["has_query"],
    } in conn_calls
    assert {
        "action": "create_connection",
        "pg": proc_calls[0]["pg"],
        "src": proc_by_name["write_generated_rows_query_to_content"],
        "dst": proc_by_name["submit_rows_query"],
        "rels": ["success"],
    } in conn_calls
    assert {
        "action": "create_connection",
        "pg": proc_calls[0]["pg"],
        "src": proc_by_name["route_rows_payload_state"],
        "dst": proc_by_name["split_trino_response_rows_to_messages"],
        "rels": ["has_payload"],
    } in conn_calls
    assert {
        "action": "create_connection",
        "pg": proc_calls[0]["pg"],
        "src": proc_by_name["split_trino_response_rows_to_messages"],
        "dst": proc_by_name["extract_trino_row_json"],
        "rels": ["split"],
    } in conn_calls
    assert {
        "action": "create_connection",
        "pg": proc_calls[0]["pg"],
        "src": proc_by_name["extract_trino_row_json"],
        "dst": proc_by_name["write_trino_row_json_to_content"],
        "rels": ["matched"],
    } in conn_calls
    assert {
        "action": "create_connection",
        "pg": proc_calls[0]["pg"],
        "src": proc_by_name["write_trino_row_json_to_content"],
        "dst": proc_by_name["publish_one_row_to_kafka"],
        "rels": ["success"],
    } in conn_calls


def test_trino_source_can_skip_checkpoint_table_auto_create(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = {
        "name": "fortisiem_device_from_trino",
        "type": "Trino",
        "trino_endpoint": "https://trino.example.com",
        "trino_user": "admin",
        "trino_catalog": "bronze",
        "trino_schema": "fortisiem",
        "trino_table": "device__history",
        "trino_checkpoint_catalog": "bronze",
        "trino_checkpoint_schema": "checkpoints",
        "trino_checkpoint_table_name": "trino_snapshot_checkpoints",
        "trino_auto_create_checkpoint_table": False,
        "trino_columns": ["accessip", "object_id", "name", "naturalid"],
        "streams": [
            {
                "id": "device__history",
                "name": "device__history",
                "entity": {"kafka": {"topic": "bronze.fortisiem.device_history.rows", "value_format": ""}},
            }
        ],
        "kafka_output": {"topic": "bronze.fortisiem.device_history.rows"},
        "schedule": {"interval_value": 5, "interval_unit": "minutes"},
    }

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source,
        flow_id="flow-trino-fortisiem-no-create",
        nifi_conn={"endpoint": "http://nifi:8080", "auth_type": "NONE"},
        kafka_conn={"endpoint": "kafka:9092"},
    ))

    assert result["ok"] is True
    proc_calls = [c for c in call_log if c["action"] == "create_processor"]
    proc_names = {c["name"] for c in proc_calls}
    assert "write_checkpoint_create_sql" not in proc_names
    assert "submit_checkpoint_create_query" not in proc_names
