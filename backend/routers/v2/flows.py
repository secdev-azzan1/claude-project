"""Flows v2 router (`/api/v2/flows`) — T1.2b.

CRUD + validation + verb skeleton + enable/disable, mirroring
frontend/src/prototype/api.ts's flow functions (`listFlows`, `getFlow`,
`saveFlow`, `deleteFlow` (via `runFlowVerb("delete")`), `runFlowVerb`,
`setFlowEnabled`, `validateFlowNow`) and reading server-side truth off the
ported rule engines in services/adapter/{legality,validation}.py.

Deployment-engine-backed behaviour (actually talking to NiFi/Kafka
Connect) does not exist yet. Every verb, once it passes the same state-machine
guard api.ts's `getVerbBlockReason` enforces, returns `501 {"detail":
"deployment engine pending", "verb": ...}` without touching flow state — the
guard itself is real today so the UI's refusal behaviour is testable ahead of
the engine landing.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel

from db import get_db
from models.adapter import AppService, ApprovedSchema, Flow, GatewayProxy, PlatformConnection
from models.adapter.bulk_job import BULK_VERBS, TERMINAL_BULK_STATES, bulk_job_to_response
from services.adapter import bulk_runner, runtime as runtime_svc
from services.adapter.common import COLLECTIONS, audit, new_id, now_iso
from services.adapter.deployer import lifecycle
from services.runtime_recovery import APP_INSTANCE_ID
from services.adapter.deployer.connect_apply import ConnectApplyError
from services.adapter.deployer.nifi_apply import NifiApplyError
from services.adapter.legality import validate_placement
from services.adapter.validation import GatewaySnapshot, ValidationIssue, validate_flow

router = APIRouter(prefix="/api/v2/flows", tags=["flows-v2"])

# The 8 verbs api.ts's `FlowVerb` union exposes through `runFlowVerb`, minus
# "delete" (that one is `DELETE /{id}` on this router, mirroring how the
# frontend's Flows page routes it through the same verb machinery but this
# port gives it its own REST verb/method instead).
ALLOWED_VERBS = ("deploy", "redeploy", "start", "pause", "resume", "stop", "stop_clear", "undeploy")

# States getEditLockReason() / disableBlockReason() (frontend/src/pages/Flows.tsx)
# both refuse edits/disable in — a flow that is deployed and not at rest.
_LOCKED_STATES = ("Running", "Paused", "Deploying", "Degraded")


class SetEnabledRequest(BaseModel):
    enabled: bool


class ClearTopicRequest(BaseModel):
    topic: str


# --------------------------------------------------------------- loaders

async def _load_services(db: AsyncIOMotorDatabase) -> List[AppService]:
    docs = await db[COLLECTIONS.services].find({}, {"_id": 0}).to_list(None)
    return [AppService(**d) for d in docs]


async def _load_schemas(db: AsyncIOMotorDatabase) -> List[ApprovedSchema]:
    docs = await db[COLLECTIONS.schemas].find({}, {"_id": 0}).to_list(None)
    return [ApprovedSchema(**d) for d in docs]


async def _load_connections(db: AsyncIOMotorDatabase) -> List[PlatformConnection]:
    docs = await db[COLLECTIONS.connections].find({}, {"_id": 0}).to_list(None)
    return [PlatformConnection(**d) for d in docs]


async def _load_gateway(db: AsyncIOMotorDatabase) -> GatewaySnapshot:
    """`COLLECTIONS.gateway` is a single document: {proxies, certProfiles,
    allowlist} (see services/adapter/common.py's docstring)."""
    doc = await db[COLLECTIONS.gateway].find_one({}, {"_id": 0})
    doc = doc or {}
    proxies = [GatewayProxy(**p) for p in (doc.get("proxies") or [])]
    allowlist = list(doc.get("allowlist") or [])
    return GatewaySnapshot(proxies=proxies, allowlist=allowlist)


async def _get_flow_doc_or_404(db: AsyncIOMotorDatabase, flow_id: str) -> Dict[str, Any]:
    doc = await db[COLLECTIONS.flows].find_one({"id": flow_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Flow not found")
    return doc


def _issue_dict(issue: ValidationIssue) -> Dict[str, Any]:
    return {"blockId": issue.blockId, "where": issue.where, "message": issue.message}


# ----------------------------------------------------------- guard mirrors

def _get_edit_lock_reason(flow: Flow) -> Optional[str]:
    """Line-for-line port of api.ts's `getEditLockReason`: editing a deployed,
    non-stopped flow is refused (kc "Save is live" is the sole exception,
    noted in the message but not modeled as a bypass here — no kc-only fast
    path exists yet)."""
    if flow.state in _LOCKED_STATES:
        return "Editing a deployed flow is refused until it is stopped. (kc sink subscriptions save live.)"
    return None


def _get_enable_block_reason(flow: Flow, enabled: bool) -> Optional[str]:
    """Port of frontend/src/pages/Flows.tsx's `bulkBlockReason`/`disableBlockReason`
    guards around `setFlowEnabled`: already-enabled/-disabled is a no-op
    refusal, and disabling is refused while the flow is not at rest."""
    if enabled:
        return "Already enabled." if flow.enabled else None
    if not flow.enabled:
        return "Already disabled."
    if flow.state in _LOCKED_STATES:
        return "Stop the flow first."
    return None


def _get_verb_block_reason(
    flow: Flow,
    verb: str,
    services: List[AppService],
    schemas: List[ApprovedSchema],
    gateway: GatewaySnapshot,
    connections: List[PlatformConnection],
) -> Optional[str]:
    """Line-for-line port of api.ts's `getVerbBlockReason` (the `editVerbs`
    table), for the 8 verbs this router serves. `delete` is intentionally
    absent — it is `DELETE /{id}` below."""
    deployed = bool(flow.deployedAt)

    if verb == "deploy":
        if flow.state in ("Running", "Paused"):
            return "Stop the flow before deploying."
        if flow.state == "Deploying":
            return "A deploy is already in progress."
        issues = validate_flow(flow, services, schemas, gateway)
        if issues:
            return f"{len(issues)} validation issue(s) — run Validate for details."
        return None

    if verb == "start":
        if not deployed:
            return "Deploy the flow first."
        if not flow.enabled:
            return "The flow is disabled."
        if flow.state == "Running":
            return "Already running."
        if flow.state == "Paused":
            return "Use Resume — the flow is paused, its trigger still fires."
        missing = [
            t
            for t in ("nifi", "kafka", "apicurio")
            if not any(c.type == t and c.active and c.health == "Healthy" for c in connections)
        ]
        if missing:
            return f"Runtime connections unavailable: {', '.join(missing)}."
        return None

    if verb == "pause":
        return None if flow.state == "Running" else "Only a running flow can be paused."

    if verb == "resume":
        return None if flow.state == "Paused" else "Only a paused flow can be resumed."

    if verb in ("stop", "stop_clear"):
        return None if flow.state in ("Running", "Paused", "Degraded") else "The flow is not running."

    if verb == "redeploy":
        if not deployed:
            return "The flow has never been deployed."
        if flow.state != "Stopped":
            return "Redeploy requires the flow stopped (and queues cleared)."
        return None

    if verb == "undeploy":
        if not deployed:
            return "The flow is not deployed."
        if flow.state in ("Running", "Paused"):
            return "Stop the flow before undeploying."
        return None

    return "Unknown verb."


# --------------------------------------------------------------------- CRUD

@router.get("/")
async def list_flows_v2(db: AsyncIOMotorDatabase = Depends(get_db)):
    return await db[COLLECTIONS.flows].find({}, {"_id": 0}).to_list(None)


@router.get("/{flow_id}")
async def get_flow_v2(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await _get_flow_doc_or_404(db, flow_id)


@router.post("/")
async def save_flow_v2(flow_in: Flow, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Create-or-update mirror of api.ts's `saveFlow` (there is no separate
    v2 create endpoint — a flow with an id not yet on record is created,
    one already on record is updated), with two server-side guards the
    frontend mock never needed because its UI only ever constructs legal
    trees and hides the editor while a flow is locked:

      1. Edit-lock: refused (409) while the *existing* record is deployed
         and not at rest — `_get_edit_lock_reason`.
      2. Structural legality: `validate_placement` (R1-R8) plus the
         flow-level (non-block) issues from `validate_flow` must both be
         clean, or the save is refused (422) with the issue messages.
         Block-level completeness issues (missing service, unset path, ...)
         do NOT block a save — those are legitimate "still editing" states,
         surfaced instead by `POST /{id}/validate` and enforced for real at
         `deploy`.
    """
    # API clients may omit/blank the id (the frontend always generates one
    # client-side); a blank id must never become a stored document key.
    if not (flow_in.id or "").strip():
        flow_in.id = new_id("flow")

    existing = await db[COLLECTIONS.flows].find_one({"id": flow_in.id}, {"_id": 0})

    if existing:
        existing_flow = Flow(**existing)
        lock_reason = _get_edit_lock_reason(existing_flow)
        if lock_reason:
            raise HTTPException(status_code=409, detail=lock_reason)
        # M12: MVP §7.1 invariant 2 — "names freeze at Deploy... for the
        # flow's lifetime". `deployedAt` set means this flow (Stopped or
        # not — a Stopped, deployed flow otherwise passes the edit-lock
        # check above and IS allowed structural edits) has derived real
        # topic/DLQ/connector names from its current name at least once;
        # renaming it now would silently orphan those names (proven live —
        # docs/orchestration/reviews/flow-engine-review.md M12) since
        # nothing re-derives or migrates them. The frontend already
        # disables the name input in this state; this is the server-side
        # backstop for API clients that bypass it.
        if existing_flow.deployedAt and existing_flow.name != flow_in.name:
            raise HTTPException(status_code=409, detail="Names freeze at deploy — undeploy first to rename.")

    services = await _load_services(db)
    schemas = await _load_schemas(db)
    gateway = await _load_gateway(db)

    placement_violations = validate_placement(flow_in)
    flow_level_issues = [i for i in validate_flow(flow_in, services, schemas, gateway) if i.blockId is None]
    if placement_violations or flow_level_issues:
        issues = [{"blockId": v.blockId, "where": None, "message": v.message} for v in placement_violations]
        issues += [_issue_dict(i) for i in flow_level_issues]
        raise HTTPException(status_code=422, detail={"issues": issues})

    now = now_iso()
    next_doc = flow_in.model_dump()
    next_doc["updatedAt"] = now

    if existing:
        next_doc["createdAt"] = existing.get("createdAt") or now
        await db[COLLECTIONS.flows].update_one({"id": flow_in.id}, {"$set": next_doc})
        await audit(db, action="Draft saved", target=flow_in.name, object="Flow")
    else:
        next_doc["createdAt"] = next_doc.get("createdAt") or now
        await db[COLLECTIONS.flows].insert_one(next_doc)
        await audit(db, action="Flow created", target=flow_in.name, object="Flow")

    return await db[COLLECTIONS.flows].find_one({"id": flow_in.id}, {"_id": 0})


@router.delete("/{flow_id}")
async def delete_flow_v2(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Mirror of api.ts's `runFlowVerb("delete")`. Performs a full teardown
    (undeploy: delete the NiFi PG + Connect connectors, empty owned Kafka
    topics) before removing the flow/runtime docs when the flow is
    deployed — `services/adapter/deployer/lifecycle.py`'s `delete()` does
    the actual work; a flow that was never deployed just has its docs
    removed, same as before."""
    doc = await _get_flow_doc_or_404(db, flow_id)
    try:
        result = await lifecycle.delete(db, doc)
    except (NifiApplyError, ConnectApplyError, lifecycle.LifecycleError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return result


# --------------------------------------------------------------------- verbs

# Dispatch table: verb -> lifecycle.<fn>(db, flow_doc) -> updated flow doc.
_VERB_HANDLERS = {
    "deploy": lifecycle.deploy,
    "redeploy": lifecycle.redeploy,
    "start": lifecycle.start,
    "pause": lifecycle.pause,
    "resume": lifecycle.resume,
    "stop": lifecycle.stop,
    "stop_clear": lifecycle.stop_clear,
    "undeploy": lifecycle.undeploy,
}


# ------------------------------------------------------------- bulk jobs
#
# A bulk run is a background job rather than N synchronous requests because
# deploy is slow (nifi_apply polls up to 30s for a parameter context, 45s for
# controller services, per flow). Doing it in the browser meant the tab could
# not be closed and gave no progress. Job state lives in Mongo so a refresh
# reattaches and a restart leaves a readable `interrupted` record.


class BulkJobRequest(BaseModel):
    verb: str
    flowIds: List[str]


@router.post("/bulk", status_code=202, summary="Start a background bulk verb run")
async def start_bulk_job_v2(payload: BulkJobRequest, db: AsyncIOMotorDatabase = Depends(get_db)):
    verb = (payload.verb or "").strip()
    if verb not in BULK_VERBS:
        raise HTTPException(status_code=422, detail=f"Unknown bulk verb: {verb}")
    if not payload.flowIds:
        raise HTTPException(status_code=422, detail="No flows selected.")

    docs = await db[COLLECTIONS.flows].find({"id": {"$in": payload.flowIds}}, {"_id": 0}).to_list(None)
    if not docs:
        raise HTTPException(status_code=404, detail="None of the selected flows exist.")

    # Preserve the caller's ordering so the UI's list matches the run order.
    by_id = {str(doc.get("id")): doc for doc in docs}
    ordered = [by_id[fid] for fid in payload.flowIds if fid in by_id]

    job = await bulk_runner.create_bulk_job(
        db, verb=verb, flow_docs=ordered, owner_instance_id=APP_INSTANCE_ID
    )
    await audit(
        db,
        f"Bulk {verb} started",
        f"{len(ordered)} flow(s)",
        object="Flow",
    )
    bulk_runner.launch_bulk_job(db, job["id"])
    return {"jobId": job["id"]}


@router.get("/bulk/queue", summary="Flow operation queue")
async def list_bulk_queue_v2(db: AsyncIOMotorDatabase = Depends(get_db)):
    """Return queued/running jobs plus a small recent terminal history."""
    docs = await db[COLLECTIONS.bulk_jobs].find(
        {}, {"_id": 0}, sort=[("created_at", -1)]
    ).to_list(100)
    return [bulk_job_to_response(doc) for doc in docs]


@router.get("/bulk/active", summary="The bulk run currently in flight, if any")
async def get_active_bulk_job_v2(db: AsyncIOMotorDatabase = Depends(get_db)):
    """Lets the Flows page reattach to a running job after a refresh -- without
    this, progress would be lost the moment the tab reloaded."""
    doc = await db[COLLECTIONS.bulk_jobs].find_one(
        {"status": {"$in": ["queued", "pending", "running"]}}, {"_id": 0}, sort=[("created_at", -1)]
    )
    return bulk_job_to_response(doc) if doc else None


@router.get("/bulk/{job_id}", summary="Bulk run status")
async def get_bulk_job_v2(job_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await db[COLLECTIONS.bulk_jobs].find_one({"id": job_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Bulk job not found")
    return bulk_job_to_response(doc)


@router.post("/bulk/{job_id}/cancel", summary="Request cancellation of a bulk run")
async def cancel_bulk_job_v2(job_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await db[COLLECTIONS.bulk_jobs].find_one({"id": job_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Bulk job not found")
    if doc.get("status") in TERMINAL_BULK_STATES:
        raise HTTPException(status_code=409, detail="That run has already finished.")
    if doc.get("status") != "queued":
        raise HTTPException(status_code=409, detail="A running operation cannot be cancelled.")

    # A queued job is cancelled atomically, so the worker cannot claim it
    # after this update. All child items remain untouched.
    await db[COLLECTIONS.bulk_jobs].update_one(
        {"id": job_id, "status": "queued"},
        {
            "$set": {
                "status": "cancelled",
                "cancel_requested": True,
                "updated_at": now_iso(),
                "finished_at": now_iso(),
            }
        },
    )
    updated = await db[COLLECTIONS.bulk_jobs].find_one({"id": job_id}, {"_id": 0})
    return bulk_job_to_response(updated)


@router.post("/{flow_id}/verbs/{verb}")
async def run_flow_verb_v2(flow_id: str, verb: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    if verb not in ALLOWED_VERBS:
        raise HTTPException(status_code=404, detail=f"Unknown verb: {verb}")

    doc = await _get_flow_doc_or_404(db, flow_id)
    flow = Flow(**doc)

    services = await _load_services(db)
    schemas = await _load_schemas(db)
    gateway = await _load_gateway(db)
    connections = await _load_connections(db)

    reason = _get_verb_block_reason(flow, verb, services, schemas, gateway, connections)
    if reason:
        await audit(
            db,
            action=f"Flow {verb} refused",
            target=flow.name,
            status="Failed",
            details=reason,
            object="Flow",
        )
        raise HTTPException(status_code=409, detail=reason)

    handler = _VERB_HANDLERS[verb]
    try:
        return await handler(db, doc)
    except lifecycle.DeployPreflightFailed as exc:
        raise HTTPException(status_code=422, detail={"rows": exc.rows}) from exc
    except (NifiApplyError, ConnectApplyError, lifecycle.LifecycleError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/{flow_id}/dedup-cache/clear")
async def clear_dedup_cache_v2(
    flow_id: str,
    block_id: str = Query(..., alias="blockId"),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """T7.4: bump the dedup cache epoch for one block — see
    `services/adapter/deployer/lifecycle.py`'s `clear_dedup_cache()`
    docstring for why this is a redeploy-scoped epoch bump rather than an
    instantaneous Redis flush (Redis is not reachable from this host)."""
    doc = await _get_flow_doc_or_404(db, flow_id)
    try:
        return await lifecycle.clear_dedup_cache(db, doc, block_id)
    except lifecycle.LifecycleError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/{flow_id}/blocks/{block_id}/test")
async def test_block_v2(flow_id: str, block_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """T7.5: bounded live probe (<=10 records, no commits) for http-read
    blocks. jdbc/kafka-read block tests (and any other http mode) still
    501 -- their compiler support doesn't exist yet either. A block that
    hosts no Test section at all (every write, kafka_kc, kc) refuses with
    422, mirroring the builder UI's own `hostsTest` rule. See
    `services/adapter/runtime.py::test_block` for the implementation."""
    doc = await _get_flow_doc_or_404(db, flow_id)
    try:
        result = await runtime_svc.test_block(db, doc, block_id)
    except runtime_svc.BlockNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except runtime_svc.BlockNotTestRunnable as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except runtime_svc.BlockTestPlaceholders as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except runtime_svc.BlockTestUnsupported as exc:
        return JSONResponse(status_code=501, content={"detail": str(exc), "blockId": block_id})
    return result


@router.post("/{flow_id}/enabled")
async def set_flow_enabled_v2(flow_id: str, body: SetEnabledRequest, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await _get_flow_doc_or_404(db, flow_id)
    flow = Flow(**doc)

    reason = _get_enable_block_reason(flow, body.enabled)
    if reason:
        raise HTTPException(status_code=409, detail=reason)

    now = now_iso()
    await db[COLLECTIONS.flows].update_one({"id": flow_id}, {"$set": {"enabled": body.enabled, "updatedAt": now}})
    await audit(db, action="Flow enabled" if body.enabled else "Flow disabled", target=flow.name, object="Flow")
    return {"id": flow_id, "enabled": body.enabled}


@router.post("/{flow_id}/validate")
async def validate_flow_v2(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Server-truth mirror of api.ts's `validateFlowNow` — same `{blockId,
    where, message}[]` shape `ValidationIssue[]` is on the frontend."""
    doc = await _get_flow_doc_or_404(db, flow_id)
    flow = Flow(**doc)

    services = await _load_services(db)
    schemas = await _load_schemas(db)
    gateway = await _load_gateway(db)

    issues = validate_flow(flow, services, schemas, gateway)
    return [_issue_dict(i) for i in issues]


# ------------------------------------------------------------- observability
# T7.5 — real runtime observability. Every function backing these lives in
# services/adapter/runtime.py; see that module's docstring for the shapes
# and the honest-values rationale (NEVER fake zeros).

@router.get("/{flow_id}/dlq")
async def get_flow_dlq_v2(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await _get_flow_doc_or_404(db, flow_id)
    return await runtime_svc.get_flow_dlq(db, doc)


@router.get("/{flow_id}/metrics")
async def get_flow_metrics_v2(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await _get_flow_doc_or_404(db, flow_id)
    return await runtime_svc.get_flow_metrics(db, doc)


@router.get("/{flow_id}/messages")
async def get_flow_messages_v2(flow_id: str, topic: str = Query(""), db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await _get_flow_doc_or_404(db, flow_id)
    try:
        return await runtime_svc.get_topic_messages(db, doc, topic)
    except runtime_svc.TopicNotOwned as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{flow_id}/topics/clear")
async def clear_flow_topic_v2(flow_id: str, body: ClearTopicRequest, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Alpha parity: the ops-view "Clear Topics" destructive action
    (MVP §19.7) -- owned topics only (an unowned topic 404s, an adopted one
    409s), count -> clear -> count against the live cluster, audited.
    See `services/adapter/runtime.py::clear_topic` for the full contract."""
    doc = await _get_flow_doc_or_404(db, flow_id)
    try:
        return await runtime_svc.clear_topic(db, doc, body.topic)
    except runtime_svc.TopicNotOwned as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except runtime_svc.TopicClearRefused as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except runtime_svc.TopicClearFailed as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/{flow_id}/runtime")
async def get_flow_runtime_v2(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Doubles as the live "refresh from NiFi" action — the frontend GETs
    this same route for both the initial load and the explicit Refresh
    button (api.ts's `getFlowRuntime`/`refreshFlowRuntime`)."""
    doc = await _get_flow_doc_or_404(db, flow_id)
    runtime = await runtime_svc.read_runtime(db, doc)
    if not runtime:
        raise HTTPException(status_code=404, detail="No runtime record for this flow")
    return runtime


@router.post("/{flow_id}/runtime/repair")
async def force_repair_flow_runtime_v2(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await _get_flow_doc_or_404(db, flow_id)
    try:
        return await runtime_svc.repair_runtime(db, doc)
    except runtime_svc.RuntimeRepairRefused as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
