# Graph Report - claude-project  (2026-08-26)

## Corpus Check
- 396 files · ~468,300 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 5466 nodes · 15751 edges · 224 communities (158 shown, 66 thin omitted)
- Extraction: 96% EXTRACTED · 4% INFERRED · 0% AMBIGUOUS · INFERRED: 576 edges (avg confidence: 0.89)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Navigation & Sidebar UI
- Schema Ceremony & App Layout
- Flow Designer Page
- App Shell & Adapter Chips
- Flows Router (Backend)
- Flow Compiler Core
- NiFi Flow Generator
- Adapter Metadata & Block UI
- NiFi Flow Manager & V2 Flow Tests
- Gateway Router (APISIX)
- Flow Builder Canvas
- Flow Model & Adapter Runtime
- Destinations & Flow Settings UI
- Application Service Model
- DB & Orphan Cleanup
- Kafka Connect Deployer
- NiFi Per-Stream PG Generation
- Block Form & Dedup Rules UI
- V2 Gateway Tests
- Deployer Orchestration
- Content Store Service
- Preflight & Test Panel UI
- Flow Graph & Branches UI
- V2 OpenAPI Tests
- Schemas Router (Apicurio)
- Flow Import API & Tests
- Kafka Topic Lifecycle
- Export Snapshot Non-Mutation Tests
- Flows V2 Mutations
- Adapter Connection & Gateway Models
- Adapter Model Mirrors (Audit/Dashboard/Flow)
- Ceremony Dialog Form
- Schema Inference UI
- Schema Inference Backend
- Transform Rules & Adapter Rule Tests
- Sources Router
- Kafka-KC Adapter Compiler
- Connections Router V2
- NiFi Apply (Deployer)
- OpenAPI Specs Router
- V2 Service Test Clients
- NiFi REST Client
- Application Services Router V2
- Connections Router V1
- Flow Legality Rules
- Architecture & Behavioral Audit Docs
- Schema Artifact Model
- Dashboard Summary & Flow Delete
- HTTP Adapter Compiler
- V2 Services Tests
- Resilience Test Fakes
- JDBC Adapter Compiler
- Stream Flow Map UI
- Frontend API Client Layer
- Source Filter Rules
- Avro Schema Editor Logic
- Iceberg Sink Lifecycle Tests
- Iceberg Sink Model & Service
- Source Entity Config Model
- Flow Import Finalize
- Test Flow Export
- Kafka Connect Client
- Kafka Schema Consumer
- Schema Types
- Test Flow Import Credentials
- Test Pagination Optional Maxpages
- Transforms
- Test Flow Import Preview
- Nifi Global Services
- Test Nifi Apply
- Validation
- Seeds
- Schema Inferencer
- Test Flow Import Finalize
- Iceberg Sinks Api
- Connection Lifecycle Runner
- Nifi Flow Manager
- Use Toast
- Test Nifi Flow Generator Trino
- Iceberg Sink Config
- Schema Inference Runner
- Test Flow Trino Schema Optional
- Test Adapter Rules
- Rest Stream Request Resolver
- Gateway Iceberg Journey
- Backfill Connection Provenance
- Blocks Kafka
- Flow Engine Review
- Flow Import Preview
- Connection
- Nifi Services
- Naming
- Flow Designer
- Test Connection Multi
- Test Iceberg Sinks Sync
- Rest Stream Branch Validation
- Flow Designer
- Rapid7 Tier12 Plan
- Journey A E
- Iceberg Sinks
- Test Connection Fingerprint
- Inference Preflight
- Schema Api
- Test Connection Resolver
- Test Schema Inference Validation
- Flow File Compactor
- Test Apicurio Client
- Test Runtime Concurrency
- Nifi Flow Generator
- Avro Display Fields
- Journey C D
- Source
- Routing
- Test Iceberg Sinks Sync
- Mock Data
- Page Search
- Rest Stream Polling Validation
- Flow Secret Refs
- Flow Runtime Errors
- Rest Stream Behavior
- Test Flow Run Metrics
- Chart
- Test Nifi Apply Live
- Test Import Atomicity
- Test Sources Test Stream Rest Methods
- Flow Designer Connection State
- Test Sources Test Stream Rest Methods
- Flow Designer Schema Requirement
- Test Schema Service Concurrency
- Diag Invokehttp
- Rest Stream Value Mapping
- Stream Migration
- Flow Engine Review
- Orphaned Artifact
- Test Local Env Loading
- Flow Engine Review
- Test Iceberg Sinks Lifecycle
- Use Toast
- Store
- Content Store
- Application Services
- Flow Designer Connection State
- Flow Table Summary
- Flow Bulk Selection
- Schema Layout
- Setup
- Alert Dialog
- Flow Operation Claims
- Test V2 Services
- App
- Schema Types
- Smb Path Normalization
- Flow Export
- Iceberg Catalog Client
- Nifi Client
- Golden Flow
- Flow Bulk Selection
- live/__init__.py
- resilience/__init__.py
- Pagination Fields
- Command
- Toast
- Toggle
- adapter common helpers module
- Gateway
- Gateway
- adapter package init (rule-engine source
- Aspect Ratio
- Connectors
- Dlq
- Ir
- Transforms
- Application Services
- Application Services
- Flow Import Credentials
- Flow Import Preview
- Flow Runtime Errors
- Flow Secret Refs
- Http Tls
- Openapi Parser
- Orphan Registry
- Blocks Http
- Blocks Jdbc
- Blocks Kafka
- Blocks Kafka Kc
- Compile Flow
- compiler package init
- Context Menu
- Drawer
- Flow Api
- Form
- Seeds
- Accordion
- Alert
- Avatar
- Badge
- Breadcrumb
- Card
- Chart
- Collapsible
- Table
- Tabs
- Tooltip
- Mock Data
- Main
- Hover Card
- Schema Inference
- Pagination
- Popover
- Resizable
- Schema Editor Barrel Export (index.ts)
- Select
- services package init
- Sonner
- Switch
- Textarea
- Use Mobile

## God Nodes (most connected - your core abstractions)
1. `cn() classname merge utility` - 279 edges
2. `Flows router` - 133 edges
3. `COLLECTIONS` - 76 edges
4. `test_content_store.FakeDB` - 69 edges
5. `compile_flow()` - 67 edges
6. `nifi_api_request` - 67 edges
7. `generate_and_deploy_flow() (service)` - 64 edges
8. `FlowDesigner()` - 62 edges
9. `_get()` - 61 edges
10. `Sources router` - 58 edges

## Surprising Connections (you probably didn't know these)
- `Alpha Backend Functional Map` --semantically_similar_to--> `store.ts (localStorage-backed mock repository)`  [INFERRED] [semantically similar]
  docs/orchestration/analysis/alpha-backend.md → frontend/src/prototype/store.ts
- `buildRuntimes() function` --implements--> `Platform Connections (6 types, one-active-per-type, fingerprint/repoint)`  [INFERRED]
  frontend/src/prototype/seeds.ts → docs/orchestration/analysis/product-requirements.md
- `CONNECT_PLUGIN_CATALOG constant` --conceptually_related_to--> `Platform Connections (6 types, one-active-per-type, fingerprint/repoint)`  [INFERRED]
  frontend/src/prototype/seeds.ts → docs/orchestration/analysis/product-requirements.md
- `ApprovedSchema interface` --shares_data_with--> `Schema ceremony (Declare→Orchestrate→Review→Approve = register)`  [INFERRED]
  frontend/src/prototype/types.ts → docs/orchestration/analysis/product-requirements.md
- `DriftFinding interface` --conceptually_related_to--> `Platform Connections (6 types, one-active-per-type, fingerprint/repoint)`  [INFERRED]
  frontend/src/prototype/types.ts → docs/orchestration/analysis/product-requirements.md

## Import Cycles
- 3-file cycle: `backend/services/adapter/compiler/__init__.py -> backend/services/adapter/compiler/compile_flow.py -> backend/services/adapter/compiler/blocks_kafka_kc.py -> backend/services/adapter/compiler/__init__.py`

## Hyperedges (group relationships)
- **Adapter package camelCase/ISO-string/extra=allow convention** — adapterflowpy_flow, backend_models_adapter_connection_platformconnection, backend_models_adapter_gateway_gatewayproxy, backend_models_adapter_runtime_flowruntime, backend_models_adapter_schema_approvedschema, backend_models_adapter_service_appservice [EXTRACTED 1.00]
- **Secret-key redaction pattern for config dicts** — backend_models_adapter_secrets_secretconfigkeys, backend_models_adapter_secrets_redact_config, backend_models_adapter_connection_platformconnection, backend_models_adapter_service_appservice [EXTRACTED 1.00]
- **Standalone scripts bypass db.py's shared async Mongo accessor** — backend_db_getdb, cleanuporphansources_script, backfillconnectionprovenance_main [INFERRED 0.75]
- **Content-store mirror kept in sync by every mutating router** — backend_routers_flows, backend_routers_sources, backend_routers_schemas, backend_routers_openapi_specs, backend_services_content_store_sync, backend_routers_content_store [INFERRED 0.85]
- **Convergent paths to a Verified, Apicurio-registered schema version** — backend_routers_schemas_verify_version, backend_routers_schema_inference_accept_inference_schema, backend_routers_flows_ensure_deploy_schemas_registered, backend_services_apicurio_client_register_schema, backend_models_schema_artifact_schemaversion [INFERRED 0.85]
- **Connection endpoint/auth changes gated through repoint lifecycle to avoid orphaned dependents** — backend_routers_connections_update_connection, backend_routers_connections_delete_connection, backend_routers_connections_repoint_connection, backend_services_connection_lifecycle_runner_run_repoint, backend_models_orphaned_artifact_orphanedartifact [INFERRED 0.75]
- **Flow -> DeploymentPlan compilation pipeline** — backend_services_adapter_compiler_compile_flow_compile_flow, backend_services_adapter_compiler_compile_flow_compile_block, backend_services_adapter_compiler_blocks_http_compile_read, backend_services_adapter_compiler_blocks_jdbc_compile_entry, backend_services_adapter_compiler_blocks_kafka_compile_entry, backend_services_adapter_compiler_blocks_kafka_kc_compile_envelope, backend_services_adapter_compiler_dlq_build, backend_services_adapter_compiler_connectors_build_kafka_kc_connector, ir_blockbuilder [EXTRACTED 1.00]
- **v2 routers sharing COLLECTIONS/audit()/new_id() from adapter common** — adaptercommon_collections, adaptercommon_audit, adaptercommon_new_id, backend_routers_v2_audit_module, backend_routers_v2_connections_module, backend_routers_v2_flows_module, backend_routers_v2_gateway_module, backend_routers_v2_schemas_module, servicesv2_module, backend_routers_v2_dashboard_module [EXTRACTED 1.00]
- **Independently-duplicated logic across router/compiler layers (proxy resolution, session-token login, JSONPath extraction)** — gateway_block_proxy_id, backend_routers_v2_connections_connection_dependents, blockshttp_session_login, backend_routers_v2_services_test_http_session_token, backend_routers_webhooks_get_json_path_value, backend_routers_v2_services_resolve_jsonpath, backend_routers_v2_schemas_resolve_record_path [INFERRED 0.85]
- **Dedup cache epoch used as a deferred-flush substitute for unreachable Redis** — backend_services_adapter_compiler_transforms_compile_dedup, backend_services_adapter_deployer_lifecycle_clear_dedup_cache, backend_services_adapter_deployer_lifecycle_bump_changed_dedup_epochs, backend_services_adapter_deployer_lifecycle_dedup_epoch_bump_updates [INFERRED 0.85]
- **Layered deploy safety net: preflight compile check, live apply, post-apply validation gate** — backend_services_adapter_deployer_lifecycle_preflight_rows, backend_services_adapter_deployer_lifecycle_deploypreflightfailed, backend_services_adapter_deployer_nifi_apply_apply_plan, backend_services_adapter_deployer_nifi_apply_await_valid_processors [INFERRED 0.85]
- **Per-instance fingerprint reused for deploy provenance, runtime drift detection, and repoint safety** — backend_services_connection_fingerprint_probe_nifi, backend_services_adapter_deployer_lifecycle_stamp_provenance, backend_services_adapter_runtime_read_runtime, backend_services_connection_lifecycle_runner_run_repoint [INFERRED 0.85]
- **Flowpack Export/Import Pipeline** — backend_services_flow_export_module, backend_services_flow_import_preview_module, backend_services_flow_import_credentials_module, backend_services_flow_import_finalize_module, backend_services_flow_secret_refs_module [INFERRED 0.85]
- **Iceberg Sink Deployment Pipeline** — backend_services_iceberg_sinks_module, backend_services_iceberg_sink_config_module, backend_services_kafka_connect_client_module, backend_services_kafka_client_module, backend_services_orphan_registry_module [INFERRED 0.85]
- **NiFi Process Group Lifecycle Control** — backend_services_nifi_client_module, backend_services_nifi_flow_manager_module, backend_services_nifi_flow_generator_module, backend_services_nifi_global_services_module [INFERRED 0.85]
- **REST Stream Config Validation & Migration Pipeline** — backend_services_rest_stream_behavior_infer_rest_stream_behaviors, backend_services_rest_stream_branch_validation_validate_rest_branch_graph, backend_services_rest_stream_pagination_validation_validate_rest_stream_pagination, backend_services_rest_stream_polling_validation_validate_rest_stream_polling, backend_services_stream_migration_migrate_source [INFERRED 0.85]
- **Schema Inference Job Deploy/Collect/Recover Lifecycle** — backend_services_schema_inference_runner_run_inference_background, backend_services_runtime_recovery_reconcile_runtime_state, backend_tests_resilience_conftest_resiliencefakedb, backend_services_schema_inference_runner_delete_inference_nifi_pg [INFERRED 0.75]
- **Fault-Injection Rollback/Atomicity Test Harness** — backend_tests_resilience_conftest_faultinjectingcollection, backend_tests_resilience_test_deploy_recovery, backend_tests_resilience_test_export_snapshot, backend_tests_resilience_test_import_atomicity, backend_tests_resilience_test_fault_fixtures [INFERRED 0.85]
- **Event-gated concurrency tests proving single-winner/idempotent-recovery semantics** — backend_tests_resilience_test_runtime_concurrency, backend_tests_resilience_test_schema_service_concurrency, backend_tests_resilience_test_inference_recovery [INFERRED 0.75]
- **Three-stage flowpack import pipeline: preview -> validate credentials -> finalize** — backend_tests_test_flow_import_preview, backend_tests_test_flow_import_credentials, backend_tests_test_flow_import_finalize, backend_services_flow_import_preview_service, backend_services_flow_import_credentials_service, backend_services_flow_import_finalize_service [EXTRACTED 1.00]
- **lifecycle.deploy() orchestrates NiFi apply + Kafka Connect connectors + topic reservation** — adapter_deployer_lifecycle, adapter_deployer_nifi_apply, adapter_deployer_connect_apply, adapter_deployer_topics, backend_tests_test_deployer [EXTRACTED 1.00]
- **Shared REST Pagination Test Harness** — backend_tests_test_nifi_flow_generator_per_stream_pg, backend_tests_test_pagination_header_and_body, backend_tests_test_pagination_loop_wiring, backend_tests_test_pagination_optional_maxpages, backend_tests_test_pagination_stop_rules [INFERRED 0.85]
- **REST Stream Validation & Behavior Layer** — backend_services_rest_stream_behavior_service, backend_services_rest_stream_branch_validation_service, backend_services_rest_stream_pagination_validation_service, backend_services_rest_stream_polling_validation_service, backend_services_rest_stream_request_resolver_service [INFERRED 0.80]
- **Fail-Closed Handling of External System Responses** — backend_services_adapter_deployer_nifi_apply_deployer, backend_services_iceberg_sinks_service, backend_services_kafka_connect_client_service [INFERRED 0.75]
- **Per-adapter-type dispatch pattern (http/jdbc/kafka/kc)** — ADAPTER_META_data, HttpSettings_component, JdbcSettings_component, KafkaReadSettings_component, KcSettings_component [INFERRED 0.85]
- **v2 router tests mount a standalone FastAPI app because server.py does not wire v2 routers in yet** — backend_tests_test_v2_connections, backend_tests_test_v2_flows, backend_tests_test_v2_gateway, backend_tests_test_v2_services [EXTRACTED 1.00]
- **Shared FaultInjectingCollection-based FakeDB pattern across the v2 test suite** — backend_tests_test_v2_connections, backend_tests_test_v2_flows, backend_tests_test_v2_runtime, backend_tests_test_v2_openapi, backend_tests_test_v2_schemas [EXTRACTED 1.00]
- **Avro Schema Ceremony Pipeline (evidence-driven Declare→Review editing)** — frontend_src_components_flow_builder_ceremonydialog_component, frontend_src_components_schema_editor_sampleinferencepanel_component, frontend_src_components_schema_editor_avroeditortabs_component, frontend_src_components_schema_editor_schemafieldlist_schemafieldlist, frontend_src_components_schema_editor_avrodisplayfields_recordtodisplayfields [INFERRED 0.85]
- **Flow Canvas Structural Editing Gated by Legality Rules** — frontend_src_components_flow_builder_flowmapview_component, frontend_src_components_flow_builder_graph_buildflowgraph, frontend_src_prototype_legality_computeaddmenu, frontend_src_prototype_legality_canreparent [INFERRED 0.85]
- **Independent Stream Flow Graph Visualization (parallel to block-flow canvas)** — frontend_src_components_flow_map_streamflowmap_component, frontend_src_components_flow_map_streamflownode_streamflownode, frontend_src_lib_streamgraph_buildstreamgraph [INFERRED 0.75]
- **Radix-primitive wrapper pattern (forwardRef + cn + displayName)** — frontend_src_components_ui_accordion_component, frontend_src_components_ui_alert_component, frontend_src_components_ui_avatar_component, frontend_src_components_ui_badge_component, frontend_src_components_ui_checkbox_component, frontend_src_components_ui_dialog_component, contextmenu_component [INFERRED 0.85]
- **Components reusing Button's CVA style variants (buttonVariants)** — frontend_src_components_ui_button_component, alertdialog_component, frontend_src_components_ui_calendar_component, frontend_src_components_ui_carousel_component [EXTRACTED 1.00]
- **Ported Avro schema editor buffer contract (types + hook + barrel, sourced verbatim from legacy Schemas page)** — schemaeditorindex_barrel, frontend_src_components_schema_editor_schematypes_module, frontend_src_components_schema_editor_useavrobuffer_hook, frontend_src_pages_schemas_page [EXTRACTED 1.00]
- **shadcn/ui Radix-primitive wrapper pattern (forwardRef + cn + displayName)** — drawer_Drawer, dropdown-menu_DropdownMenu, hover-card_HoverCard, frontend_src_components_ui_menubar_menubar, frontend_src_components_ui_navigation_menu_navigationmenu, popover_Popover, frontend_src_components_ui_progress_progress, frontend_src_components_ui_radio_group_radiogroup, frontend_src_components_ui_scroll_area_scrollarea, select_Select, frontend_src_components_ui_separator_separator, sheet_Sheet, frontend_src_components_ui_slider_slider, frontend_src_components_ui_label_label [INFERRED 0.85]
- **SidebarContext hub consumed via useSidebar across the sidebar component family** — frontend_src_components_ui_sidebar_sidebarcontext, frontend_src_components_ui_sidebar_sidebarprovider, sidebar_useSidebar, frontend_src_components_ui_sidebar_sidebar, frontend_src_components_ui_sidebar_sidebartrigger, frontend_src_components_ui_sidebar_sidebarrail, frontend_src_components_ui_sidebar_sidebarmenubutton [EXTRACTED 1.00]
- **Radix Slot-based asChild polymorphic component pattern** — frontend_src_components_ui_form_formcontrol, frontend_src_components_ui_sidebar_sidebargrouplabel, frontend_src_components_ui_sidebar_sidebargroupaction, frontend_src_components_ui_sidebar_sidebarmenubutton, frontend_src_components_ui_sidebar_sidebarmenusubbutton [INFERRED 0.85]
- **Toast Notification System** — frontend_src_components_ui_toast_toastcomponents, toast_toasttypes, toaster_toaster, usetoasthook_usetoast, usetoasthook_toast, usetoastui_barrel [INFERRED 0.95]
- **Layered API Client Architecture** — api_apiclient, applicationservicesapi_applicationservicesapi, flowapi_flowsapi, flowapi_sourcesapi [INFERRED 0.85]
- **Pure Logic Modules With Colocated Tests** — defaultrouteaction_normalizedefaultrouteaction, flowbulkselection_getvisibleselectionstate, flowdesignerconnectionstate_mergesourceconnectionstate [INFERRED 0.75]
- **Avro <-> structured field bidirectional conversion pipeline** — frontend_src_lib_schemaeditor_normalizeavrorecord, frontend_src_lib_schemaeditor_avrotostructuredfields, frontend_src_lib_schemaeditor_structuredtoavrofields [INFERRED 0.85]
- **Flow import preview-to-finalize readiness pipeline** — frontend_src_lib_flowapi_summarizeimportpreviewissues, frontend_src_lib_flowapi_createimportschemaresolutions, frontend_src_lib_flowapi_isflowimportready, frontend_src_lib_flowimportcache_refreshimportedflowqueries [INFERRED 0.85]
- **Preflight validation before triggering a stateful action** — frontend_src_lib_icebergsinksapi_preflightresult, frontend_src_lib_inferencepreflight_collectmissingpathparamresolutionsforinference, frontend_src_lib_inferenceapi_client [INFERRED 0.75]
- **Dependent-guarded destructive/disruptive admin actions** — frontend_src_pages_connections_platformconnectionspanel, frontend_src_pages_appservices_appservicespage, frontend_src_pages_apisix_apisixpage [INFERRED 0.85]
- **Stream graph build-and-render pipeline** — frontend_src_lib_streamgraph_buildstreamgraph, frontend_src_lib_streamflowmap_streamstoflowgraph, frontend_src_pages_flowdesigner_flowdesignerpage [INFERRED 0.85]
- **Sync-safe read-cache warming across pure helpers** — frontend_src_prototype_api_readcachewarmpattern, frontend_src_prototype_api_getverbblockreason, frontend_src_prototype_api_geteditlockreason, frontend_src_prototype_api_validateflownow, frontend_src_prototype_api_connectiondependents, frontend_src_prototype_api_proxydependents, frontend_src_prototype_api_servicedependents [INFERRED 0.85]
- **Unified branch vocabulary spanning creation, migration, and naming** — frontend_src_prototype_mutations_setbranch, frontend_src_prototype_migrate_migratebranches, frontend_src_prototype_branches_branchesof, frontend_src_prototype_naming_branchpathlabels [INFERRED 0.85]
- **Shared flowId+blockId identity linking a schema to its owning block** — frontend_src_pages_flows_schemastatus, frontend_src_pages_schemas_schemas, frontend_src_prototype_api_toapprovedschema [INFERRED 0.75]
- **Dedup design confirmed across MVP spec, two independent NiFi references, and compiler+decisions** — architecturemvp_dedupmechanism, deduprefflow_doc, userrefflows2_doc, compilerspec_doc, docs_orchestration_decisions_doc [INFERRED 0.85]
- **Schema ceremony redesign: alpha's verify/register gap drives product spec, decision D10, and prototype UI audit** — productreq_schemaceremony, alphafrontend_doc, docs_orchestration_decisions_doc, prototypeui_doc [INFERRED 0.85]
- **seeds.ts + store.ts + types.ts together form the prototype's entire mock backend** — frontend_src_prototype_seeds_file, frontend_src_prototype_store_file, frontend_src_prototype_types_file [EXTRACTED 1.00]
- **Kafbat topic-creation defect simultaneously blocked Journeys A, B, and C/D deploys** — defect_kafbat_topic_create, journeyae_report, journeyb_report, journeycd_report [EXTRACTED 1.00]
- **Flow-engine review findings tracked in verification-state and closed out via Journey R re-verification** — flowenginereview_doc, verificationstate_doc, journeyrreverify_report [EXTRACTED 1.00]
- **Iceberg/Apicurio connector config defect found in Journey A, fixed, and confirmed live in Journey R (R4)** — defect_iceberg_connector_config, defect_apicurio_converter_url, journeyae_report, journeyrreverify_report [EXTRACTED 1.00]

## Communities (224 total, 66 thin omitted)

### Community 0 - "Navigation & Sidebar UI"
Cohesion: 0.02
Nodes (150): DrawerPortal, DropdownMenu, mainItems, systemItems, AddBlockMenu(), NavLink, NavLinkCompatProps, AccordionContent (+142 more)

### Community 1 - "Schema Ceremony & App Layout"
Cohesion: 0.03
Nodes (124): Declare→Orchestrate→Review→Approve Schema Ceremony, AppLayout(), Props, AppSidebar(), CeremonyDialog, recordToDisplayFields, RadioGroup, RadioGroupItem (+116 more)

### Community 2 - "Flow Designer Page"
Cohesion: 0.02
Nodes (123): RFC-5988, ApiKeyLocation, AttributeExtractionRule, BodyFormat, BranchingMode, buildKafkaTopicName(), buildPaginationCfg(), ConnectionRecord (+115 more)

### Community 3 - "App Shell & Adapter Chips"
Cohesion: 0.03
Nodes (97): Flows.test.tsx test suite, App(), queryClient, ADAPTER_META, AdapterChip(), labelMap, StatusBadge(), Variant (+89 more)

### Community 4 - "Flows Router (Backend)"
Cohesion: 0.05
Nodes (106): Flows router, _append_flow_run_start(), _attr_token(), _build_default_metrics(), _build_kafka_topic_metrics(), clear_flow_kafka_messages(), _clear_nifi_pg_if_missing(), _clear_stale_nifi_pg_reference() (+98 more)

### Community 5 - "Flow Compiler Core"
Cohesion: 0.07
Nodes (95): FlowBlock, See types.ts FlowBlock doc comment for the per-adapter `config` payload…, compile_flow(), CompileContext, Everything `compile_flow` needs beyond the `Flow` document itself, assembled by…, api_key_query_ctx(), _dedup_rule(), golden_ctx() (+87 more)

### Community 6 - "NiFi Flow Generator"
Cohesion: 0.05
Nodes (95): _apply_child_path_param_defaults(), _apply_root_path_param_defaults(), _apply_stream_routing(), _augment_url_with_pagination(), _augment_url_with_stream_query(), _body_format_for_stream(), _body_template_for_stream(), _build_default_attribute_props() (+87 more)

### Community 7 - "Adapter Metadata & Block UI"
Cohesion: 0.05
Nodes (78): ADAPTER_META (per-adapter icon/label/description/tint table), HttpSettings (per-adapter settings for http), JdbcSettings (per-adapter settings for jdbc), KafkaReadSettings (per-adapter settings for kafka read), KcSettings (per-adapter settings for kc sink), ServiceSelector (existing-vs-manual service picker), AdapterChip component, AddBlockMenu component (+70 more)

### Community 8 - "NiFi Flow Manager & V2 Flow Tests"
Cohesion: 0.07
Nodes (86): services/nifi_flow_manager.py (get_processor_config), _clear_overrides(), FakeDB, _make_client(), TestClient, Tests for T1.2b: routers/v2/flows.py, routers/v2/dashboard.py,…, M12 — MVP §7.1 invariant 2: names freeze at Deploy. A Stopped, deployed flow…, The name-freeze guard must not become a blanket edit-lock -- a Stopped,… (+78 more)

### Community 9 - "Gateway Router (APISIX)"
Cohesion: 0.05
Nodes (84): _apisix_conn(), _block_proxy_id(), create_cert_profile(), _default_cert_expiry(), delete_cert_profile(), delete_proxy(), _fail_reconcile(), get_gateway_state() (+76 more)

### Community 10 - "Flow Builder Canvas"
Cohesion: 0.06
Nodes (76): Canvas Creates Structure, Form Configures It, AddBlockMenu, DestinationsPanel, FlowMapView / FlowMapViewInner, deletePreview(), FlowMapView(), FlowMapViewInner(), FlowSettingsForm (+68 more)

### Community 11 - "Flow Model & Adapter Runtime"
Cohesion: 0.05
Nodes (85): Flow, Mirrors types.ts `Flow` exactly for every field the frontend reads or writes,…, _all_connector_names(), _as_int(), BlockNotFound, BlockNotTestRunnable, BlockTestError, BlockTestPlaceholders (+77 more)

### Community 12 - "Destinations & Flow Settings UI"
Cohesion: 0.05
Nodes (69): DestinationsPanel(), FlowSettingsFormProps, Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle (+61 more)

### Community 13 - "Application Service Model"
Cohesion: 0.06
Nodes (76): audit() function, COLLECTIONS class (v2 Mongo collection names), new_id() function, ApplicationService, ApplicationServiceCreate, ApplicationServiceUpdate, normalize_application_service_config(), _positive_int() (+68 more)

### Community 14 - "DB & Orphan Cleanup"
Cohesion: 0.04
Nodes (66): AuditEvent (adapter model), One-time cleanup: delete source records not referenced by any flow. A source is…, db.get_db (Mongo dependency), AsyncIOMotorDatabase, AuditEvent (legacy model), AuditEventCreate, BaseModel, PlatformSettings (+58 more)

### Community 15 - "Kafka Connect Deployer"
Cohesion: 0.07
Nodes (79): now_iso(), ISO-8601 UTC timestamp with millisecond precision and a trailing "Z", matching…, create_connectors (Connect connector create+pause), delete_connectors, Any, DeploymentPlan.connectors -> live Kafka Connect connectors (T7.2). Thin…, Create (or upsert) every connector in `connectors`, then immediately pause each…, Delete every named connector. A connector that is already gone… (+71 more)

### Community 16 - "NiFi Per-Stream PG Generation"
Cohesion: 0.10
Nodes (72): generate_and_deploy_flow() (service), Translate a Source config into a live NiFi process group. Steps: 1. Get NiFi…, _fake_nifi_env(), _make_failing_conn_mock(), _make_kafka_conn(), _make_nifi_conn(), _make_source(), Tests for per-stream NiFi process group topology in REST API flows. (+64 more)

### Community 17 - "Block Form & Dedup Rules UI"
Cohesion: 0.06
Nodes (64): Dedup Always Pinned Last (MVP Ruling 12), dedupWindow.test.ts test suite, BlockForm, BlockForm.tsx (flow builder block form), hostsTest(), noTestReason(), FlowSettingsForm(), KvRows (+56 more)

### Community 18 - "V2 Gateway Tests"
Cohesion: 0.08
Nodes (53): _apisix_connection_doc(), FakeCollection, FakeCursor, FakeDB, FakeHttpClient, _get_nested(), _install_apisix_deletes(), _install_apisix_puts() (+45 more)

### Community 19 - "Deployer Orchestration"
Cohesion: 0.09
Nodes (64): services/adapter/compiler (compile_flow), async_test(), FakeDB, _http_kafka_flow(), _jdbc_read_flow(), _kafka_read_flow(), _kc_flow(), _patch_ensure_topics_ok() (+56 more)

### Community 20 - "Content Store Service"
Cohesion: 0.10
Nodes (68): application_services_path(), _assert_inside_root(), atomic_write_json(), _bounded_path_segment(), _bundle_id(), _canonical_flows(), ContentStoreReport, _default_nifi_service() (+60 more)

### Community 21 - "Preflight & Test Panel UI"
Cohesion: 0.06
Nodes (58): Apisix page render test, PreflightDialog, PreflightDialog(), TestPanel, MUTATING, ResponseTree, TestPanel(), TestPanelProps (+50 more)

### Community 22 - "Flow Graph & Branches UI"
Cohesion: 0.07
Nodes (57): BlockFormProps, BranchesCard(), BranchesCardProps, CeremonyDialogProps, BlockNode (FlowMapView), buildFlowGraph, canHostRouting, chainTipIds (+49 more)

### Community 23 - "V2 OpenAPI Tests"
Cohesion: 0.12
Nodes (57): server.app (real FastAPI app instance), _clear_overrides(), FakeDB, _make_client(), TestClient, Tests for the v2 OpenAPI subsystem (backend/routers/v2/openapi.py). Uses…, _small_doc_bytes(), test_get_spec_and_operations_404_for_unknown_spec() (+49 more)

### Community 24 - "Schemas Router (Apicurio)"
Cohesion: 0.09
Nodes (57): _active_apicurio_connection(), _apicurio_auth(), approve_schema(), _as_records(), _ccompat_base(), _ccompat_delete(), _check_compatibility(), _coerce_int() (+49 more)

### Community 25 - "Flow Import API & Tests"
Cohesion: 0.05
Nodes (49): Flow Import Credential Helpers Tests, Flow Import Finalize Helpers Tests, Flow Import Preview Helpers Tests, ApiFlow, ApiSource, ControllerService, ControllerServiceConfig, ControllerServiceDescriptor (+41 more)

### Community 26 - "Kafka Topic Lifecycle"
Cohesion: 0.09
Nodes (53): count_topic, delete_topic (Kafbat DELETE), empty_topic, ensure_topics, list_topics (M13 Kafbat listing), Any, DeploymentPlan.topics -> live Kafka topics (T7.2). Thin orchestration over…, Delete `topic` entirely (used by `lifecycle.delete` — DLQ + owned data topics —… (+45 more)

### Community 27 - "Export Snapshot Non-Mutation Tests"
Cohesion: 0.07
Nodes (42): test_export_snapshot.py (export non-mutation tests), _database_snapshot(), test_export_does_not_mutate_database_or_content_store(), test_export_read_failure_is_non_mutating_and_retryable(), test_runtime_state_change_during_export_does_not_leak_into_package(), _tree_snapshot(), async_test(), populate_flow_bundle_db() (+34 more)

### Community 28 - "Flows V2 Mutations"
Cohesion: 0.09
Nodes (52): clear_dedup_cache_v2(), clear_flow_topic_v2(), ClearTopicRequest, delete_flow_v2(), force_repair_flow_runtime_v2(), _get_edit_lock_reason(), _get_enable_block_reason(), get_flow_dlq_v2() (+44 more)

### Community 29 - "Adapter Connection & Gateway Models"
Cohesion: 0.05
Nodes (30): Flow (adapter model), PlatformConnection, Any, BaseModel, Adapter-model mirror of frontend/src/prototype/types.ts (connections section).…, Mirrors types.ts `PlatformConnection`. `config` holds non-secret fields on the…, Browser-safe dict: known secret keys in `config` become `None`, and a…, GatewayProxy (+22 more)

### Community 30 - "Adapter Model Mirrors (Audit/Dashboard/Flow)"
Cohesion: 0.07
Nodes (46): Adapter-model mirror of frontend/src/prototype/types.ts `AuditEvent`. Not a re-…, DashboardSummary, BaseModel, Adapter-model mirror of the `DashboardSummary` interface returned by…, BlockTestResult, BranchCondition, BranchInfo, FlowTopic (+38 more)

### Community 31 - "Ceremony Dialog Form"
Cohesion: 0.07
Nodes (45): FormField, useFormField, CeremonyDialog(), PLACEHOLDER_FIELDS, schemaOption(), STEPS, templateOption(), KvRow (+37 more)

### Community 32 - "Schema Inference UI"
Cohesion: 0.08
Nodes (48): RFC-4180, SampleInferencePanel, SampleInferencePanel(), handleInferredTemplate, BuildContext, buildFields(), checkValue(), CsvColumn (+40 more)

### Community 33 - "Schema Inference Backend"
Cohesion: 0.08
Nodes (49): AuditEvent, BaseModel, BaseModel, Schema Inference Job model., SchemaInferenceJob, Schema Inference router, accept_inference_schema(), AcceptSchemaRequest (+41 more)

### Community 34 - "Transform Rules & Adapter Rule Tests"
Cohesion: 0.09
Nodes (13): `config` is a loosely typed per-kind payload — see types.ts TransformRule doc…, TransformRule, make_block(), make_flow(), make_topic(), TestAddMenu, TestCanReparent, TestDerivedNames (+5 more)

### Community 35 - "Sources Router"
Cohesion: 0.09
Nodes (49): Sources router, create_source(), _decode_bytes_with_encoding(), _decode_preview_bytes(), delete_source(), _derive_smb_target(), _extract_mongo_template_variables(), get_source() (+41 more)

### Community 36 - "Kafka-KC Adapter Compiler"
Cohesion: 0.09
Nodes (42): compile_publish(), FlowBlock, `kafka_kc` adapter compilation — compiler-spec.md §3.4 (FULL scope, T7.1).…, _compile_block(), _compile_kc(), Flow, FlowBlock, `compile_flow(flow, ctx) -> DeploymentPlan` — the T7.1 compiler entry point.… (+34 more)

### Community 37 - "Connections Router V2"
Cohesion: 0.10
Nodes (46): activate_connection_v2(), connection_dependents(), _count(), delete_connection_v2(), _from_client_result(), _get_conn_or_404(), get_connection_impact_v2(), list_connections_v2() (+38 more)

### Community 38 - "NiFi Apply (Deployer)"
Cohesion: 0.12
Nodes (46): AppliedResult, _apply_controller_services(), _apply_intra_connections(), apply_plan (DeploymentPlan -> live NiFi), _apply_port_links(), _apply_ports(), _apply_processors(), _auth() (+38 more)

### Community 39 - "OpenAPI Specs Router"
Cohesion: 0.10
Nodes (43): OpenAPI Specs router, attach_openapi_to_source(), AttachSpecRequest, _compress_b64(), detach_openapi_from_source(), get_openapi_spec(), get_operation_detail(), list_operations() (+35 more)

### Community 40 - "V2 Service Test Clients"
Cohesion: 0.13
Nodes (36): Server-side port of frontend/src/prototype's flow-model rule engines. This…, services/apicurio_client.py (register_schema, test_apicurio_connection), services/apisix_client.py (put_upstream, put_route, delete_route, delete_upstream, test_admin), services/kafka_client.py (test_kafka_connection, test_kafbat_connection), services/kafka_connect_client.py (test_kafka_connect_connection), services/nifi_client.py (test_nifi_connection), _clear_overrides(), _deployed_flow() (+28 more)

### Community 41 - "NiFi REST Client"
Cohesion: 0.08
Nodes (37): _classify_nifi_http_response(), get_nifi_root_process_group_result(), _get_nifi_token(), _nifi_sni_error(), _nifi_ui_path_error(), _normalize_nifi_base_url(), Any, Response (+29 more)

### Community 42 - "Application Services Router V2"
Cohesion: 0.10
Nodes (40): _classify_http_response(), _has_any_secret(), list_services(), _parse_host_port(), Any, AsyncIOMotorDatabase, post, Response (+32 more)

### Community 43 - "Connections Router V1"
Cohesion: 0.11
Nodes (40): Connections router, activate_connection(), _connection_name_exists(), create_connection(), delete_connection(), get_connection(), get_connection_impact(), _has_dependents_on_active() (+32 more)

### Community 44 - "Flow Legality Rules"
Cohesion: 0.12
Nodes (40): add_intent(), add_menu_label(), AddMenuEntry, block_by_id(), can_reparent, children_of(), compute_add_menu, compute_root_menu() (+32 more)

### Community 45 - "Architecture & Behavioral Audit Docs"
Cohesion: 0.11
Nodes (39): Alpha Backend Functional Map, Alpha Frontend Behavioral Audit, Adapter model (base/http/jdbc/kafka/kafka_kc/kc), Deduplication transform (SHA-256, Redis, per-stream cache, TTL), DLQ & failure taxonomy (record/run/infrastructure failures), Core Architecture Analysis — Data Mobility Platform MVP, Naming tokenizer (trim→lowercase→underscore, single normalization rule), Record Envelope (ingest_id/ingest_ts/op metadata, excluded from dedup) (+31 more)

### Community 46 - "Schema Artifact Model"
Cohesion: 0.13
Nodes (36): FlowSchemaLink, BaseModel, field_validator, SchemaArtifact, SchemaArtifactCreate, SchemaField, SchemaVersion, Schemas router (+28 more)

### Community 47 - "Dashboard Summary & Flow Delete"
Cohesion: 0.08
Nodes (19): get_summary(), Return platform-wide KPI stats., FakeCollection, FakeCursor, FakeInsertResult, FakeUpdateResult, _field_value(), _matches() (+11 more)

### Community 48 - "HTTP Adapter Compiler"
Cohesion: 0.13
Nodes (38): _apply_auth(), _base_url_expr(), _build_pagination(), _build_query(), _build_session_login(), _build_trigger(), _compile_lookup(), compile_read() (+30 more)

### Community 49 - "V2 Services Tests"
Cohesion: 0.16
Nodes (36): FakeDB, FakeResponse, _make_client(), patch_httpx(), patch_tcp(), parametrize, TestClient, Tests for the v2 Application Services subsystem… (+28 more)

### Community 50 - "Resilience Test Fakes"
Cohesion: 0.09
Nodes (16): tests/resilience/conftest.py (ResilienceFakeDB, FaultInjectingCollection), assert_no_transitional_state(), FakeCursor, FakeResult, FaultInjectingCollection (fake Mongo collection with injectable failures), FaultPoint (StrEnum of injectable failure points), _get_nested(), _matches() (+8 more)

### Community 51 - "JDBC Adapter Compiler"
Cohesion: 0.11
Nodes (27): compile_entry(), _compile_lookup(), _compile_read(), _compile_write(), _ensure_db_pool(), AppService, FlowBlock, Tail (+19 more)

### Community 52 - "Stream Flow Map UI"
Cohesion: 0.10
Nodes (29): StreamFlowMap, nodeTypes, StreamFlowMap(), StreamFlowMapInner(), StreamFlowMapProps, StreamFlowMap.test, streams, StreamFlowNodeBody (+21 more)

### Community 53 - "Frontend API Client Layer"
Cohesion: 0.08
Nodes (33): api Client Object (get/post/put/patch/delete/postForm), ApiError Class, getApiBase() Function, Adapter UI Prototype Is Offline-Only (PROTOTYPE_OFFLINE guard), request() Internal Fetch Wrapper, applicationServicesApi Object, flowsApi Object, getFlowExportFilename() Function (+25 more)

### Community 54 - "Source Filter Rules"
Cohesion: 0.09
Nodes (11): FilterRule, field_validator, model_validator, Include/exclude filter on a stream (non-branching). Orthogonal to routes., SourceCreate (pydantic model), StreamPagination, StreamParameterBinding, test_source_create_accepts_split_trino_checkpoint_table_config() (+3 more)

### Community 55 - "Avro Schema Editor Logic"
Cohesion: 0.12
Nodes (33): useAvroBuffer(), rapid7AssetsAvro mock schema, ApiSchemaVersion type, applyNullable(), AVRO_LOGICAL_TYPES, AVRO_PRIMITIVE_TYPES, AvroField, avroFromVersion (+25 more)

### Community 56 - "Iceberg Sink Lifecycle Tests"
Cohesion: 0.09
Nodes (25): models.connection.ConnectionUpdate, services.kafka_connect_client, services.nifi_client, FakeAuditCollection, FakeDB, FakeResponse, FakeSinkCollection, Regression tests for Kafka Connect failure handling in the Iceberg sink… (+17 more)

### Community 57 - "Iceberg Sink Model & Service"
Cohesion: 0.12
Nodes (33): IcebergSink, BaseModel, build_config_for_sink, delete_sinks_for_flow, deploy_sinks_for_flow, disable_sink, enable_sink, find_shared_table_conflicts() (+25 more)

### Community 58 - "Source Entity Config Model"
Cohesion: 0.09
Nodes (19): EntityConfig, EntityIcebergConfig, ExtractionRule, KafkaOutput, BaseModel, Marks this stream as a route-child of another stream., Per-entity-stream intent to sync its Kafka topic into an Iceberg table., Present on entity (Kafka-publishing leaf) streams. (+11 more)

### Community 59 - "Flow Import Finalize"
Cohesion: 0.16
Nodes (34): _application_service_docs(), _artifact_prefix(), _created_result(), _derive_kafka_topic(), _derive_schema_artifact_id(), _designer_entity_config(), _designer_entity_destination(), finalize_flow_import (+26 more)

### Community 60 - "Test Flow Export"
Cohesion: 0.15
Nodes (29): export_flow(), to_canonical_json(), to_jsonable(), build_flow_export_package, _bundle_id(), _checksum(), dumps_export_package(), export_filename() (+21 more)

### Community 61 - "Kafka Connect Client"
Cohesion: 0.12
Nodes (33): Kafka Connect router, get_cluster(), get_orphans(), AsyncIOMotorDatabase, _auth_headers(), delete_connector, _error_message(), get_cluster_info (+25 more)

### Community 62 - "Kafka Schema Consumer"
Cohesion: 0.15
Nodes (32): _coerce_scalar(), _collect_xml_hints(), _collect_xml_hints_from_element(), consume_from_kafka(), consume_messages_for_inference, consume_via_kafbat, _decode_payload_bytes(), _detect_format() (+24 more)

### Community 63 - "Schema Types"
Cohesion: 0.17
Nodes (27): AvroEditorTabs(), AvroEditorTabs, SchemaFieldList, SchemaFieldRow, SchemaNodeEditor, ADVANCED_TYPE_OPTION, applyFieldType(), createDefaultField (+19 more)

### Community 64 - "Test Flow Import Credentials"
Cohesion: 0.16
Nodes (26): Flow Import router, finalize_flowpack_import(), preview_flowpack_import(), AsyncIOMotorDatabase, post, UploadFile, validate_flowpack_import_credentials(), _validate_flowpack_upload() (+18 more)

### Community 65 - "Test Pagination Optional Maxpages"
Cohesion: 0.12
Nodes (29): routers.schema_inference, _has_text(), _metadata_source(), _positive_int(), _ptype(), Any, ValueError, Validation helpers for REST stream pagination configuration. (+21 more)

### Community 66 - "Transforms"
Cohesion: 0.12
Nodes (31): dedupe_preserve_order(), ensure_json_record_services(), escape_el_literal(), format_duration_hours(), Escape a literal for use inside a NiFi EL `'...'` string argument., Render a fractional-hours window as a NiFi duration string. Whole hours render…, Add (once per group) a generic JsonTreeReader/JsonRecordSetWriter pair. Every…, build_chain (transform rule compiler) (+23 more)

### Community 67 - "Test Flow Import Preview"
Cohesion: 0.26
Nodes (30): FlowImportPreviewError, ValueError, test_content_store.FakeDB, async_test(), _checksum(), make_flowpack(), make_flowpack_with_secret_refs(), test_import_preview_router_rejects_wrong_extension() (+22 more)

### Community 68 - "Nifi Global Services"
Cohesion: 0.26
Nodes (30): get_controller_service_config, Fetch full configuration and property descriptors for a single controller…, _collect_app_service_references(), _collect_nifi_service_references(), create_global_service, delete_global_service, enrich_global_service, _fetch_live_service() (+22 more)

### Community 69 - "Test Nifi Apply"
Cohesion: 0.12
Nodes (27): BlockGroup, ConnectionSpec, ParameterContextSpec, One wire inside a BlockGroup. `from_`/`to` reference a processor `key`, or one…, _ensure_parameter_context(), _param_value_for_nifi (M8 fix), M8 fix: a `None`-valued parameter serializes as JSON `null` in NiFi's…, _update_parameter_context() (+19 more)

### Community 70 - "Validation"
Cohesion: 0.15
Nodes (29): is_valid_cron(), topic_name_collision, block_proxy_id, _branch_incomplete, dedup_stream_not_per_record_reason, deploy_preflight, gateway_refusals(), _is_kafka_family_write() (+21 more)

### Community 71 - "Seeds"
Cohesion: 0.12
Nodes (28): buildRuntimes(), buildSeedState(), daysAgo(), diverged(), FS_INCIDENT_AVRO, hoursAgo(), JSON_READER, kafkaClientProps() (+20 more)

### Community 72 - "Schema Inferencer"
Cohesion: 0.19
Nodes (14): Avro nested-record naming: full field-path names + uniqueness safety net, add_scalar_example(), AvroBuilder (class), clone_node(), infer_avro_schema(), merge_into(), new_node(), Node (+6 more)

### Community 73 - "Test Flow Import Finalize"
Cohesion: 0.22
Nodes (27): async_test(), credential_values(), package_with_designer_payload(), package_with_helper_primary_and_entity_child(), package_with_runtime_state(), package_with_schema_version_4(), raw_package(), test_finalize_import_clears_runtime_and_schema_inference_state() (+19 more)

### Community 74 - "Iceberg Sinks Api"
Cohesion: 0.08
Nodes (24): api, ApiError, BASE, getApiBase(), normalizeErrorDetail(), request(), IcebergSink type, IcebergSinkListResponse (+16 more)

### Community 75 - "Connection Lifecycle Runner"
Cohesion: 0.13
Nodes (26): probe_apicurio_fingerprint, probe_iceberg_fingerprint, probe_kafka_fingerprint, probe_kafka_connect_fingerprint, compute_impact, AsyncIOMotorDatabase, Impact preview service for connection operations (read-only)., Compute the impact of an operation on a connection. Args: db: MongoDB database… (+18 more)

### Community 76 - "Nifi Flow Manager"
Cohesion: 0.14
Nodes (27): clear_process_group_queues, _collect_pg_connection_ids(), _collect_pg_controller_services(), _collect_pg_processors(), deactivate_process_group, _drop_connection_queue(), _list_pg_flow(), prepare_process_group_for_start() (+19 more)

### Community 77 - "Use Toast"
Cohesion: 0.12
Nodes (24): Toast, ToastAction, ToastActionElement, ToastClose, ToastDescription, ToastProps, ToastTitle, toastVariants (+16 more)

### Community 78 - "Test Nifi Flow Generator Trino"
Cohesion: 0.10
Nodes (19): services/adapter/compiler/ir.py (BlockBuilder, ProcessorSpec), routers.flows, services.adapter.deployer.nifi_apply, services.kafka_client, services.nifi_flow_generator, services.rest_stream_behavior, test_deployer.py (referenced, not read), test_rest_deploy_repair_does_not_add_fanout_to_route_child() (+11 more)

### Community 79 - "Iceberg Sink Config"
Cohesion: 0.14
Nodes (24): build_connector_config, derive_connector_name, _derive_control_group_id_prefix(), derive_table_name(), derive_table_name_from_names, normalize_iceberg_identifier(), Any, Pure-function generator for Iceberg Sink Kafka Connect connector configs. No… (+16 more)

### Community 80 - "Schema Inference Runner"
Cohesion: 0.14
Nodes (24): Any, substitute_runtime_values(), _compile_rest_source_for_inference(), _create_kafka_inference_topic(), _extract_required_runtime_bindings(), services/schema_inference_runner.py (_compile_rest_source_for_inference, INFERENCE_SAMPLE_LIMIT, _inference_collection_complete), _normalize_attr_token(), _normalize_inferred_schema_root() (+16 more)

### Community 81 - "Test Flow Trino Schema Optional"
Cohesion: 0.12
Nodes (17): Flow (legacy model), FlowCreate (legacy model), FlowRun, models.flow.FlowCreate, BaseModel, field_validator, _is_trino_source_type(), _route_graph_edges() (+9 more)

### Community 82 - "Test Adapter Rules"
Cohesion: 0.10
Nodes (14): ApprovedSchema, types.ts `ApprovedSchema`, plus additive server-only `draftAvro` /…, Flow, FlowBlock, FlowTopic, Tests for backend/services/adapter -- the Python port of the frontend's flow-…, Mirrors the task's `python -c "import services.adapter..."` smoke check., Backend mirror of frontend httpPathIssue(): a full URL typed into the http path… (+6 more)

### Community 83 - "Rest Stream Request Resolver"
Cohesion: 0.20
Nodes (22): _body_for_format(), _find_unresolved_placeholders(), _header_get(), _mask_header_value(), _mask_url(), Any, ValueError, resolve_rest_stream_request() (+14 more)

### Community 84 - "Gateway Iceberg Journey"
Cohesion: 0.12
Nodes (24): build_fortisiem_maximum_useful.py builder, xdr_asset unified asset inventory (SentinelOne superset entity), Datatypes Journey Report, FortiSIEM Iceberg sink historically held only one run (suspected silent error swallow), NiFi JDBC controller-service validation fails: driver jar path + empty password, Live JSON flow: HTTP read advances but Kafka publish never produces messages, None-valued parameter deletes itself from the parameter context on redeploy (M8), Sample inference emits duplicate Avro named types for repeated nested objects (gateway-iceberg DEFECT 1) (+16 more)

### Community 85 - "Backfill Connection Provenance"
Cohesion: 0.12
Nodes (22): backfill_apicurio(), backfill_kafka(), backfill_nifi(), main(), probe_apicurio_fingerprint(), probe_kafka_fingerprint(), probe_nifi_fingerprint(), Backfill connection provenance: stamp connection IDs and fingerprints on… (+14 more)

### Community 86 - "Blocks Kafka"
Cohesion: 0.18
Nodes (21): compile_entry(), compile_publish(), _compile_read_terminal(), _ensure_csv_reader(), _ensure_xml_reader(), _kafka_key_expr(), FlowBlock, Tail (+13 more)

### Community 87 - "Flow Engine Review"
Cohesion: 0.15
Nodes (23): routing.py (branch/routing compiler), transforms.py (transform chain compiler), topics.py (topic ensure/delete), naming.py (tokenize/derive-name parity), validation.py (flow validate + deploy preflight), blocks_jdbc.py (JDBC adapter compiler), blocks_kafka.py (Kafka adapter compiler), compile_flow.py (flow IR compiler) (+15 more)

### Community 88 - "Flow Import Preview"
Cohesion: 0.25
Nodes (22): _canonical_checksum(), _count_prefix(), _exported_schema_bodies(), _flow_name_exists(), _grouped_service_count(), _nested_value(), _nifi_service_resolutions(), preview_flow_import (+14 more)

### Community 89 - "Connection"
Cohesion: 0.13
Nodes (13): SECRET_CONFIG_FIELDS, SECRET_CONFIG_KEYS, Connection (legacy model), ConnectionCreate (legacy model), ConnectionRepoint, ConnectionUpdate, ConnectionLifecycleJob, BaseModel (+5 more)

### Community 90 - "Nifi Services"
Cohesion: 0.17
Nodes (18): NifiGlobalService, NifiGlobalServiceCreate, NifiGlobalServiceUpdate, BaseModel, field_validator, NiFi Services router, create_service(), delete_service() (+10 more)

### Community 91 - "Naming"
Cohesion: 0.17
Nodes (19): base_topic_name, branch_path_labels(), clean_topic_override (R7), cron_preview(), derive_topic_name, derived_topic_default(), DerivedName, _is_kafka_family_write() (+11 more)

### Community 92 - "Flow Designer"
Cohesion: 0.19
Nodes (19): NormalizedDefaultRouteAction, normalizeDefaultRouteAction(), asString(), createDefaultStream(), createExtractionRule(), createParamBinding(), createRoutingRule(), createTransformationRule() (+11 more)

### Community 93 - "Test Connection Multi"
Cohesion: 0.11
Nodes (15): ExtendedFaultInjectingCollection, FakeDB, Unreachable connection → 409 error, connection NOT activated., Reachable but Unknown fingerprint (None) with no dependents → activation…, Dependents present on active connection → existing dependent 409 still wins…, FaultInjectingCollection with update_many support., Probe function raises unexpectedly → treated as unreachable, 409 returned., Count documents matching query. (+7 more)

### Community 94 - "Test Iceberg Sinks Sync"
Cohesion: 0.35
Nodes (19): Streams live on the source, so one source edit can affect several flows., Reconcile iceberg_sinks documents for a single flow against its source's…, sync_sinks_for_flow, sync_sinks_for_source(), async_test(), FakeDB, make_flow(), make_stream() (+11 more)

### Community 95 - "Rest Stream Branch Validation"
Cohesion: 0.18
Nodes (18): _add_edge(), _extracted_attribute_names(), ValueError, Validation helpers for REST stream branch graphs., Validate nested REST branch graph structure. Branches may be nested to…, Raised when a REST stream branch graph is invalid., RestBranchValidationError, RestBranchValidationResult (+10 more)

### Community 96 - "Flow Designer"
Cohesion: 0.15
Nodes (20): appendJsonPathSegment(), asRecord(), buildResponseExplorerNode(), buildResponseInsights(), collectArraySuggestions(), collectFieldSuggestions(), dedupeFieldSuggestions(), formatKeyValueLines() (+12 more)

### Community 97 - "Rapid7 Tier12 Plan"
Cohesion: 0.13
Nodes (19): APISIX API Gateway, Browser UI Verification Pass Report, build_rapid7_asyad_maximum_useful.py builder, build_sentinelone_maximum_useful.py builder, fileshare.asset__enrich__set_key ingest_ts reference pattern, ingest_ts per-record epoch-millis stamping (SentinelOne), 90_replay isolated process group standing rule, source_object_id native-vendor-ID convention (raw.md §4/§5B) (+11 more)

### Community 98 - "Journey A E"
Cohesion: 0.18
Nodes (18): connectors.py (Kafka Connect config builder), runtime.py (metrics/messages/drift reader), Dedup suppression proof via live NiFi component counters, Names-freeze-at-Deploy invariant, Multi-processor conditional routing structure (chained vs single RouteOnAttribute), Kafka Connect converter points at ccompat, not core registry API (DEFECT 2b / E2b), Iceberg connector config omits OAuth/S3 keys (DEFECT 2 / E2 / M14), Kafbat-mode topic creation is a no-op (DEFECT 1 / E1) (+10 more)

### Community 99 - "Iceberg Sinks"
Cohesion: 0.25
Nodes (18): Iceberg Sinks router, disable_sink(), enable_sink(), get_sink_config(), _get_sink_or_404(), list_sinks(), pause_sink(), preflight_sink() (+10 more)

### Community 100 - "Test Connection Fingerprint"
Cohesion: 0.11
Nodes (16): Kafka up + cluster_id → ok True, fingerprint == cluster_id, reachable True., describe_cluster returns no cluster_id → reachable True, ok False, fingerprint…, Authorization-style error after broker responded → reachable True, ok False…, Kafbat mode, no bootstrap, Kafbat URL responds → reachable True, fingerprint…, Kafka down (NoBrokersAvailable) → reachable False, ok False., Kafbat mode WITH usable bootstrap → cluster_id returned (bootstrap wins)., NiFi API helper failure with reachable=False must not become Unknown/reachable., Auth/permission failure proves NiFi responded, but identity remains unknown. (+8 more)

### Community 101 - "Inference Preflight"
Cohesion: 0.17
Nodes (16): Shared HTTP API client (@/lib/api), icebergSinksApi client, inferenceApi client, AttributeExtractionLike, collectBranchStreamIds, collectMissingPathParamResolutionsForInference, InferencePreflightIssue, InferencePreflightStream (+8 more)

### Community 102 - "Schema Api"
Cohesion: 0.18
Nodes (14): ApiSchemaArtifact type, GenerateResponse, SchemaArtifactCreatePayload, SchemaInferPayload, SchemaInferResponse, schemasApi, UpdateVersionResponse, VerifyResponse (+6 more)

### Community 103 - "Test Connection Resolver"
Cohesion: 0.16
Nodes (10): _normalize_active_flags, Keep both the legacy and v2 active markers aligned., format_connection_labels(), get_missing_connection_types(), AsyncIOMotorDatabase, require_runtime_connections(), async_test(), FakeCollection (+2 more)

### Community 104 - "Test Schema Inference Validation"
Cohesion: 0.19
Nodes (15): routers/schema_inference.py (_missing_template_param_resolutions, _source_for_inference_validation), _inference_collection_complete(), _field(), _fortisiem_like_source(), _named_type_names(), Every Avro named-type ("record") name reachable from a schema node, recursively., Live proof: 2 real users fetched fresh from dummyjson.com, not a fixture., test_dummyjson_users_shape_produces_unique_named_types_and_parses() (+7 more)

### Community 105 - "Flow File Compactor"
Cohesion: 0.32
Nodes (15): services/flow_file_compactor.py compact_flow_file(), compact_flow_file, _compact_parameter_bindings(), _compact_schema_value(), _compact_stream(), _compact_value(), _drop_defaults(), _is_empty() (+7 more)

### Community 106 - "Test Apicurio Client"
Cohesion: 0.23
Nodes (13): async_test(), FakeResponse, _install_scripted_client(), Any, Regression test for E6: `services/apicurio_client.py::register_schema`'s dual-…, Two logical registrations -> ccompat versions [1, 2], not [1,2,3,4] (the dual-…, `ccompat_only` defaults to False -- the legacy alpha router…, `post_responses`/`get_responses` are consumed in call order (a single entry is… (+5 more)

### Community 107 - "Test Runtime Concurrency"
Cohesion: 0.32
Nodes (14): routers/flows.py start_flow/stop_flow, make_rest_source(), make_verified_rest_flow(), test_deploy_recovery.py (deploy persistence-failure & concurrency tests), _deployable_db(), _patch_deploy_prerequisites(), test_concurrent_deployments_create_only_one_process_group(), test_persistence_failure_deletes_new_process_group_and_releases_state() (+6 more)

### Community 108 - "Nifi Flow Generator"
Cohesion: 0.22
Nodes (14): _build_trino_iceberg_flow(), _create_process_group(), Create a child process group and return its ID., Build a Trino HTTP API flow that exports one Kafka message per selected Iceberg…, _trino_all_columns_query_builder_sql(), _trino_checkpoint_ref(), _trino_column_expr_builder_sql(), _trino_column_types() (+6 more)

### Community 109 - "Avro Display Fields"
Cohesion: 0.24
Nodes (11): childrenOf(), displayTypeOf(), nodeOf(), structuredToDisplayFields, AvroBuffer, BufferState, AvroRecord, describeStructuredType (+3 more)

### Community 110 - "Journey C D"
Cohesion: 0.24
Nodes (10): Apicurio Schema Registry (ccompat v7), Proxy delete never tears down live APISIX objects (DEFECT-4 / E4), DELETE schema version deletes the wrong Apicurio registry version (DEFECT-2 / fix E6), One schema registration always consumes two ccompat versions (Finding F2), Proxy delete now cleans APISIX upstream/routes (E4 fix, apisixCleaned flag), registeredVersion persistence fix (_coerce_int, template $set), register_schema(ccompat_only=True) aligns app version 1:1 with ccompat (E6 fix), E2E Journey C+D Verification Log (+2 more)

### Community 111 - "Source"
Cohesion: 0.21
Nodes (5): Stream (pydantic model), test_stream_body_format_defaults_to_empty(), test_stream_body_format_normalizes_supported_values(), test_stream_body_format_rejects_unsupported_values(), test_stream_pagination_preserves_ui_contract_fields()

### Community 112 - "Routing"
Cohesion: 0.33
Nodes (12): branch_rule_el(), EL mapping per compiler-spec §4., _branch_token(), _is_unconditional(), out_port (dedicated child output port), FlowBlock, Branch fan-out at a parent block's output — compiler-spec.md §4. Every child of…, match="any": ONE `RouteOnAttribute` with one genuine dynamic property per rule… (+4 more)

### Community 114 - "Mock Data"
Cohesion: 0.15
Nodes (12): auditLog, connections, ConnHealth, dashboardStats, flows, FlowState, flowSummary, recentActivity (+4 more)

### Community 115 - "Page Search"
Cohesion: 0.27
Nodes (11): filterFlowRows, filterSchemaArtifacts, includesQuery(), latestSchemaStatus(), matchesSchemaStatusFilter(), normalizeQuery(), SchemaArtifactStatusFilter, SearchableFlow (+3 more)

### Community 116 - "Rest Stream Polling Validation"
Cohesion: 0.27
Nodes (10): models.source.SourceCreate, ValueError, Validation helpers for deprecated REST stream polling configuration., Raised when REST polling configuration is unsafe or unsupported., RestPollingValidationError, services.rest_stream_polling_validation, _stream_id(), validate_rest_stream_polling() (rejects removed polling feature) (+2 more)

### Community 117 - "Flow Secret Refs"
Cohesion: 0.36
Nodes (11): _sanitize_payload, _extract_from_value(), extract_secret_refs_from_payloads, is_openapi_metadata_url, is_secret_key, is_secret_ref, is_sensitive_key(), Any (+3 more)

### Community 118 - "Flow Runtime Errors"
Cohesion: 0.35
Nodes (10): Any, runtime_error_from_kafka_message, runtime_error_topic, runtime_errors_from_kafka_messages(), _safe_attributes(), _safe_str(), services.flow_runtime_errors, test_runtime_error_from_kafka_message_ignores_non_failure_events() (+2 more)

### Community 119 - "Rest Stream Behavior"
Cohesion: 0.32
Nodes (10): _has_entity_output(), infer_rest_stream_behaviors(), REST stream behavior inference. This module keeps user-selected Kafka output…, Return whether this stream should publish in the current generation context., RestStreamBehavior (dataclass), _stream_id(), stream_publishes_to_kafka(), test_output_selection_is_only_entity_configuration() (+2 more)

### Community 120 - "Test Flow Run Metrics"
Cohesion: 0.25
Nodes (6): routers/flows.py run-metrics helpers, FakeCollection, FakeDB, test_append_run_uses_pre_start_kafka_baseline(), test_close_run_persists_kafka_delta(), test_records_processed_is_kafka_delta_not_lifetime_total()

### Community 121 - "Chart"
Cohesion: 0.25
Nodes (9): ChartConfig, ChartContainer, ChartContext, ChartContextProps, ChartLegendContent, ChartTooltipContent, getPayloadConfigFromPayload(), THEMES (+1 more)

### Community 122 - "Test Nifi Apply Live"
Cohesion: 0.29
Nodes (8): _delete_parameter_context(), _nifi_conn(), Flow, LIVE integration test (T7.2): applies a real, minimal http-read -> kafka write…, http read (no auth, no split, no pagination) -> kafka write. The smallest tree…, _smoke_ctx(), _smoke_flow(), test_apply_minimal_http_kafka_plan_against_live_nifi()

### Community 123 - "Test Import Atomicity"
Cohesion: 0.27
Nodes (8): InjectedFailure, RuntimeError, test_import_atomicity.py (import rollback/atomicity tests), parametrize, RacingFlowCollection (class, forces concurrent insert race), test_concurrent_imports_with_same_name_leave_one_complete_import(), test_import_removes_partial_content_store_bundle(), test_import_rolls_back_every_insert_boundary()

### Community 125 - "Flow Designer Connection State"
Cohesion: 0.27
Nodes (8): asString(), FlowDesignerConnectionPayload, FlowDesignerConnectionState, FlowDesignerSourceType, mergeSourceConnectionState(), normalizeMode(), SourceConnectionMode, SourceConnectionRecord

### Community 126 - "Test Sources Test Stream Rest Methods"
Cohesion: 0.31
Nodes (7): routers/sources.py (test_stream), BaseModel, model_validator, TestStreamRequest, async_test(), test_rest_test_stream_rejects_mutating_method_without_confirmation(), test_rest_test_stream_sends_confirmed_post_json_body()

### Community 127 - "Flow Designer Schema Requirement"
Cohesion: 0.44
Nodes (7): Flow Designer Schema Requirement Tests, FlowDesignerSchemaSourceType, FlowDesignerSchemaStatus, getEntitySchemaStatusLabel, normalizeSourceType, requiresSchemaWorkflow, shouldBlockSaveForUnverifiedSchemas

### Community 128 - "Test Schema Service Concurrency"
Cohesion: 0.32
Nodes (5): routers/schemas.py verify_version/delete_artifact, _schema_artifact(), test_concurrent_schema_verification_registers_once(), test_delete_is_rejected_while_schema_verification_is_running(), test_referenced_application_service_cannot_be_deleted()

### Community 129 - "Diag Invokehttp"
Cohesion: 0.36
Nodes (8): legality.py (placement/terminal rules), Base URL + Path raw string concatenation, no scheme/URL validation anywhere, InvokeHTTP Diagnosis Report (testflow), dummyjson.com test API, Execution Plan — Data Mobility Platform, Path field auto-strips full URL to relative path (UI fix, with toast), Implementation State Tracker, Playwright UI Journey Report

### Community 130 - "Rest Stream Value Mapping"
Cohesion: 0.36
Nodes (7): assert_no_legacy_brace_placeholders(), find_legacy_brace_placeholders(), find_placeholders(), ValueError, REST stream placeholder mapping helpers., RestStreamValueMappingError, unresolved_placeholders_in_values()

### Community 131 - "Stream Migration"
Cohesion: 0.36
Nodes (6): migrate_source(), Any, Migrate legacy source/stream documents to the new routing model. Idempotent:…, Convert old is_primary / routing_rules shape to new entity/routes/filters…, test_migrate_source_adds_primary_entity_for_legacy_source_without_entities(), test_migrate_source_does_not_add_primary_entity_when_explicit_entity_exists()

### Community 132 - "Flow Engine Review"
Cohesion: 0.43
Nodes (7): blocks_http.py (HTTP adapter compiler), api_key query-location leaks secret as a malformed HTTP header (M16), EvaluateJsonPath Return Type scalar-vs-json defect on offset/page probe (R2-D1), HTTP pagination URL never advances, loops on page 1 forever (C4), session_token login relationship auto-terminate/connect conflict (M7), Pagination fix: URL template moved onto fetch's HTTP URL property (C4 fix), Return Type scalar->json one-line fix for offset/page probe (R2-D1 fix)

### Community 133 - "Orphaned Artifact"
Cohesion: 0.40
Nodes (4): OrphanedArtifact, BaseModel, Orphaned artifact tracking model., Orphaned artifact registry service.

### Community 134 - "Test Local Env Loading"
Cohesion: 0.33
Nodes (3): server.load_environment, Path, Load backend-local env first, then repo-root env for local development.

### Community 135 - "Flow Engine Review"
Cohesion: 0.47
Nodes (6): lifecycle.py (deploy/verbs/undeploy/delete), Flow delete leaves Kafka Connect connector orphaned after repair-to-Draft (DEFECT 6), Pause/Resume broken for every root that is not http (M9), Data topic survives delete of a flow undeployed-to-Draft (teardown gap), Undeploy does not clear dedup caches (M10), Dedup epoch cache-invalidation mechanism

### Community 137 - "Use Toast"
Cohesion: 0.40
Nodes (5): Toast Primitive Family (Toast, ToastProvider, ToastViewport, ToastAction, ToastClose, ToastTitle, ToastDescription), Toaster Component, toast() Imperative Trigger Function, useToast Hook, components/ui/use-toast Re-export Barrel

### Community 138 - "Store"
Cohesion: 0.40
Nodes (6): buildSeedState() function, getState() function, load() internal function, mutate() function, resetDemoData() function, PrototypeState interface

### Community 139 - "Content Store"
Cohesion: 0.40
Nodes (5): AsyncIOMotorDatabase, post, rematerialize_content_store_endpoint(), validate_content_store_status(), test_content_store_router_validate()

### Community 140 - "Application Services"
Cohesion: 0.40
Nodes (5): create_application_service, delete_application_service, test_application_service, update_application_service, sync_application_service

### Community 141 - "Flow Designer Connection State"
Cohesion: 0.40
Nodes (5): normalizeDefaultRouteAction() Function, normalizeDefaultRouteAction Test Suite, mergeSourceConnectionState() Function, normalizeMode() Function, mergeSourceConnectionState Test Suite

### Community 142 - "Flow Table Summary"
Cohesion: 0.50
Nodes (3): summarizeList Tests, ListSummary, summarizeList

### Community 143 - "Flow Bulk Selection"
Cohesion: 0.60
Nodes (3): getVisibleSelectionState(), toggleVisibleSelection(), VisibleSelectionState

### Community 144 - "Schema Layout"
Cohesion: 0.70
Nodes (3): schemaFieldRowKey, schemaWorkspaceLayout, schemaWorkspaceLayout test suite

### Community 146 - "Alert Dialog"
Cohesion: 0.50
Nodes (4): AlertDialog UI primitive, Button UI primitive (buttonVariants CVA), Calendar UI primitive (DayPicker wrapper), Carousel UI primitive (Embla wrapper)

### Community 147 - "Flow Operation Claims"
Cohesion: 0.50
Nodes (4): Flow Operation Claims, Iceberg Sinks Service, Lifecycle Locks Service, NiFi Global Services Manager

### Community 148 - "Test V2 Services"
Cohesion: 0.50
Nodes (3): make_scripted_client(), Any, Build an httpx.AsyncClient stand-in. `responses` maps HTTP method…

### Community 149 - "App"
Cohesion: 0.50
Nodes (4): App (route table), AppLayout component, AppSidebar component (mainItems/systemItems nav), NavLink component (react-router-dom compat wrapper)

### Community 150 - "Schema Types"
Cohesion: 0.83
Nodes (4): schemaTypes.ts (type tables & node factories), useAvroBuffer hook (structured⇄raw sync buffer), Schemas.tsx (legacy source of the deep Avro editor logic), @/lib/schemaEditor (structured/Avro conversion library)

### Community 152 - "Flow Export"
Cohesion: 0.67
Nodes (3): Flow Export Service, Flow File Compactor, Flow Import Finalizer

### Community 154 - "Nifi Client"
Cohesion: 0.67
Nodes (3): get_nifi_root_process_group_id, Get the root process group ID from NiFi., NiFi Flow Generator

### Community 155 - "Golden Flow"
Cohesion: 1.00
Nodes (3): golden_flow.json (compiler DeploymentPlan fixture: rapid7/insightvm http->kafka->iceberg), iceberg_connector_bronze.fileshare.asset__history.json fixture, iceberg_connector_bronze.sentinelone.agent__history.json fixture

### Community 156 - "Flow Bulk Selection"
Cohesion: 0.67
Nodes (3): getVisibleSelectionState() Function, toggleVisibleSelection() Function, Flow Bulk Selection Test Suite

## Ambiguous Edges - Review These
- `PlatformSettings` → `_validate_verified_schema_for_flow()`  [AMBIGUOUS]
  backend/routers/settings.py · relation: conceptually_related_to
- `collectMissingPathParamResolutionsForInference` → `OpenApiOperationDetail type`  [AMBIGUOUS]
  frontend/src/lib/openapiApi.ts · relation: conceptually_related_to
- `validation.test.ts` → `Prototype UI Audit`  [AMBIGUOUS]
  docs/orchestration/analysis/prototype-ui.md · relation: conceptually_related_to
- `E2E Journey B Verification Log (Routing)` → `RouteOnAttribute wired to a 'failure' relationship it doesn't have (C2)`  [AMBIGUOUS]
  docs/orchestration/e2e/journey-b.md · relation: conceptually_related_to

## Knowledge Gaps
- **483 isolated node(s):** `queryClient`, `Props`, `mainItems`, `systemItems`, `NavLinkCompatProps` (+478 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **66 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `PlatformSettings` and `_validate_verified_schema_for_flow()`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `collectMissingPathParamResolutionsForInference` and `OpenApiOperationDetail type`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `validation.test.ts` and `Prototype UI Audit`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `E2E Journey B Verification Log (Routing)` and `RouteOnAttribute wired to a 'failure' relationship it doesn't have (C2)`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **Why does `registeredVersion persistence fix (_coerce_int, template $set)` connect `Journey C D` to `Schema Ceremony & App Layout`?**
  _High betweenness centrality (0.254) - this node is a cross-community bridge._
- **Why does `register_schema(ccompat_only=True) aligns app version 1:1 with ccompat (E6 fix)` connect `Journey C D` to `Schemas Router (Apicurio)`, `Journey A E`?**
  _High betweenness centrality (0.250) - this node is a cross-community bridge._
- **Why does `Flows router` connect `Flows Router (Backend)` to `Application Service Model`, `DB & Orphan Cleanup`, `NiFi Per-Stream PG Generation`, `Content Store Service`, `Schemas Router (Apicurio)`, `Kafka Topic Lifecycle`, `Export Snapshot Non-Mutation Tests`, `NiFi Apply (Deployer)`, `Connections Router V1`, `Schema Artifact Model`, `Dashboard Summary & Flow Delete`, `Iceberg Sink Model & Service`, `Source Entity Config Model`, `Test Flow Export`, `Test Pagination Optional Maxpages`, `Nifi Global Services`, `Nifi Flow Manager`, `Test Nifi Flow Generator Trino`, `Test Flow Trino Schema Optional`, `Rest Stream Branch Validation`, `Test Connection Resolver`, `Test Runtime Concurrency`, `Rest Stream Polling Validation`, `Flow Runtime Errors`, `Test Flow Run Metrics`?**
  _High betweenness centrality (0.098) - this node is a cross-community bridge._