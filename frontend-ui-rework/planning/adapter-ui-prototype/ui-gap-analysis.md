# UI Gap Analysis — existing app vs adapter direction

2026-08-11 · verdict per UI area for this prototype. Vocabulary:
KEEP (as-is) · MODIFY (same surface, changed content) · EXTEND (same
surface, new capabilities) · REMOVE · REPLACE (new surface for the job).

| Area | Verdict | Rationale |
|---|---|---|
| App shell / sidebar / layout | MODIFY | Same chrome; nav items renamed/reorganized (Flows, Application Services, Platform Connections promoted; Service Manager & Settings retired; Variables added); prototype badge + reset in footer. |
| Dashboard | MODIFY | Kept (open product decision documented); KPIs re-based on new nouns (Flows / Running / Approved Schemas / Connections health). "Total Sources", "Verified Schemas" retired. |
| Flow Designer (6-step wizard) | REPLACE | The stream/source-type model it encodes is gone. Replaced by the form-centric adapter Flow Builder (outline + per-block forms + compact guided visual). Route redirects. |
| Flow map (StreamFlowMap/graph libs) | EXTEND | Reused wholesale; extended with topic pill nodes, dashed kc edges, branch-name labels, legality-filtered + menus, error badges. |
| Flows list ("Flow Runner") | EXTEND | Strongest existing screen. Table/bulk/detail-sheet patterns kept; adds Pause/Resume, Stop & Clear, Redeploy verbs; Runtime Issues → DLQ tab; Processors/Services live-edit tabs → read-only Blocks & Services; flowpack → Connector export/import. |
| Test-first configuration | KEEP (rehomed) | One-test-feeds-everything, failures-as-data, response tree, detect-pagination, ${placeholder} prompts — moved into per-block Test sections. |
| Schema Manager | REPLACE | Draft→Needs-Verification→Verified workspace removed. Schemas becomes a read-only browser; authoring moves into the kafka_kc schema ceremony (which reuses the structured⇄raw editor mechanism). |
| Platform Connections | EXTEND | Best-audited area, patterns kept (typed forms, Test, write-only secrets, impact-preview delete, health badges). Extended: multiple-per-type + Active, Redis + APISIX types, Iceberg type removed, Repoint dialog, health vs reachability, gateway resources. 30s background polling removed. |
| Service Manager (NifiServices) | REPLACE | NiFi controller-service management is out of scope by spec. Replaced by Application Services (4 types incl. Database / External Kafka / Sink destination; revisions; logical retirement). HTTP-service concept survives. |
| ApplicationServices.tsx (orphaned page) | REMOVE | Superseded by the new routed Application Services page. |
| Audit Log | KEEP | Page unchanged; seed events extended with new verbs. |
| Settings | REMOVE | Was only a wrapper around Connections; Connections gets its own nav entry. |
| Iceberg sink UI (toggle + tabs) | REPLACE | Iceberg is never a toggle in the new model: kafka_kc block + Sink destination service + ceremony. Connect status appears in the flow detail Connect tab. |
| Variables | NEW | Global variables screen (spec §"Variables"); flow-level overrides in Flow settings. |
| Schema ceremony | NEW | 4-step modal wizard (Declare → Orchestrate → Review → Approve), the only path that creates schemas. |
| Destinations panel | NEW | Topics + attached sinks as a list, in the Flow Builder. |
| StatusBadge / block-reason pattern / AppLayout / react-query conventions | KEEP | The app's interaction vocabulary; extended with new statuses (Paused, Approved, Update available, Action required, Sealed, Retired…). |
| Old source types (MongoDB, SMB, Webhook, PostgreSQL, Trino as types) | REMOVE / MODIFY | Mongo+SMB → greyed "coming later" families; Webhook/syslog absent; PostgreSQL/Trino live on as jdbc dialects via Database services. |

## Biggest deltas the reviewer should look at

1. Flow Builder: wizard → outline + block forms + compact guided visual.
2. Schema authoring: standalone manager → in-flow ceremony; Schemas
   read-only.
3. Connections: singleton per type → multiple + Active + Repoint; new
   Redis/APISIX; Iceberg demoted to a service.
4. Services: NiFi plumbing UI → credential/endpoint profiles with
   revisions.
5. Vocabulary: source/stream/verified → adapter/block/entity/approved.
