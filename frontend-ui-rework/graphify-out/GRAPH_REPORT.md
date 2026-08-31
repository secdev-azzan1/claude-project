# Graph Report - src  (2026-08-29)

## Corpus Check
- 175 files · ~172,242 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1501 nodes · 4283 edges · 65 communities (59 shown, 6 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 44 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Community 0
- Community 1
- Community 2
- Community 3
- Community 4
- Community 5
- Community 6
- Community 7
- Community 8
- Community 9
- Community 10
- Community 11
- Community 12
- Community 13
- Community 14
- Community 15
- Community 16
- Community 17
- Community 18
- Community 19
- Community 20
- Community 21
- Community 22
- Community 23
- Community 24
- Community 25
- Community 26
- Community 27
- Community 28
- Community 29
- Community 30
- Community 31
- Community 32
- Community 33
- Community 34
- Community 35
- Community 36
- Community 37
- Community 38
- Community 39
- Community 40
- Community 41
- Community 42
- Community 43
- Community 44
- Community 45
- Community 46
- Community 47
- Community 48
- Community 49
- Community 50
- Community 51
- Community 52
- Community 53
- Community 54
- Community 55
- Community 56
- Community 57
- Community 58
- Community 59
- Community 60
- Community 61

## God Nodes (most connected - your core abstractions)
1. `cn()` - 294 edges
2. `FlowDesigner()` - 62 edges
3. `Flow` - 39 edges
4. `FlowBlock` - 35 edges
5. `Button` - 29 edges
6. `request()` - 28 edges
7. `Input` - 27 edges
8. `Schemas()` - 27 edges
9. `deriveTopicName()` - 23 edges
10. `FlowBuilder()` - 22 edges

## Surprising Connections (you probably didn't know these)
- `ProxyField()` --indirect_call--> `listGatewayProxies()`  [INFERRED]
  components/service-form/ServiceFormFields.tsx → prototype/api.ts
- `Section()` --calls--> `cn()`  [EXTRACTED]
  components/flow-builder/BlockForm.tsx → lib/utils.ts
- `CeremonyDialog()` --indirect_call--> `listSchemaTemplates()`  [INFERRED]
  components/flow-builder/CeremonyDialog.tsx → prototype/api.ts
- `PaginationFieldsProps` --references--> `FlowBlock`  [EXTRACTED]
  components/flow-builder/PaginationFields.tsx → prototype/types.ts
- `AlertDialogOverlay` --calls--> `cn()`  [EXTRACTED]
  components/ui/alert-dialog.tsx → lib/utils.ts

## Import Cycles
- None detected.

## Communities (65 total, 6 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.02
Nodes (83): RFC-5988, ApiKeyLocation, AttributeExtractionRule, BodyFormat, BranchingMode, buildKafkaTopicName(), ConnectionRecord, DatabaseSourceConfig (+75 more)

### Community 1 - "Community 1"
Cohesion: 0.06
Nodes (72): CeremonyDialog(), schemaOption(), templateOption(), childrenOf(), displayTypeOf(), nodeOf(), recordToDisplayFields(), structuredToDisplayFields() (+64 more)

### Community 2 - "Community 2"
Cohesion: 0.06
Nodes (53): PLACEHOLDER_FIELDS, STEPS, KvRow, KvRows(), KvRowsProps, LockedKvRow, PreflightDialog(), ALWAYS_OWNED (+45 more)

### Community 3 - "Community 3"
Cohesion: 0.06
Nodes (47): Avatar, AvatarFallback, AvatarImage, Breadcrumb, BreadcrumbEllipsis(), BreadcrumbItem, BreadcrumbLink, BreadcrumbList (+39 more)

### Community 4 - "Community 4"
Cohesion: 0.07
Nodes (43): Command, CommandDialogProps, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList, CommandSeparator (+35 more)

### Community 5 - "Community 5"
Cohesion: 0.10
Nodes (43): isAdoptedRooted(), getEditLockReason(), syncFlowTopics(), addIntent, addMenuLabel(), blockById(), canReparent(), childrenOf() (+35 more)

### Community 6 - "Community 6"
Cohesion: 0.07
Nodes (42): BULK_LABEL, BulkAction, bulkBlockReason(), chainRows(), clearTopicActionLabel(), disableBlockReason(), DisclosureRow(), downloadJson() (+34 more)

### Community 7 - "Community 7"
Cohesion: 0.08
Nodes (44): SampleInferencePanel(), RFC-4180, AvroField, BuildContext, buildFields(), checkValue(), CsvColumn, describeType() (+36 more)

### Community 8 - "Community 8"
Cohesion: 0.09
Nodes (40): buildRuntimes(), buildSeedState(), CONNECT_PLUGIN_CATALOG, daysAgo(), diverged(), FS_INCIDENT_AVRO, hoursAgo(), JSON_READER (+32 more)

### Community 9 - "Community 9"
Cohesion: 0.08
Nodes (36): Artifact, checkAvroRecord(), CheckLine, CheckPanel(), DeleteTarget, FilterChip(), PROVENANCE_META, PROVENANCE_ORDER (+28 more)

### Community 10 - "Community 10"
Cohesion: 0.08
Nodes (34): AppSidebar(), mainItems, systemItems, NavLink, NavLinkCompatProps, ThemeToggle(), Separator, Sidebar (+26 more)

### Community 11 - "Community 11"
Cohesion: 0.08
Nodes (28): AppLayout(), Props, RadioGroup, RadioGroupItem, Skeleton(), connectionToDraft(), defaultDraft(), Draft (+20 more)

### Community 12 - "Community 12"
Cohesion: 0.07
Nodes (36): buildPaginationCfg(), createEmptyEntityConfig(), defaultEntityDestinationDraft(), defaultEntityIcebergConfig(), extractMongoTemplateVariables(), extractTemplateVariables(), findInvalidKeyValueLines(), findResponseNodeByPath() (+28 more)

### Community 13 - "Community 13"
Cohesion: 0.08
Nodes (34): ApiRequestError, approveSchema(), BASE, cache, ClearTopicResult, connectorsCache, createFlow(), DashboardSummary (+26 more)

### Community 14 - "Community 14"
Cohesion: 0.11
Nodes (30): AdminAction, Apisix(), certFieldErrors(), CertForm, emptyProxyForm(), formFromProxy(), gwId(), hostFieldError() (+22 more)

### Community 15 - "Community 15"
Cohesion: 0.11
Nodes (29): addButtonClass(), deletePreview(), DropMenuState, FIT_VIEW_OPTIONS, FlowMapView(), FlowMapViewInner(), FlowMapViewProps, nodeTypes (+21 more)

### Community 16 - "Community 16"
Cohesion: 0.11
Nodes (22): nodeTypes, StreamFlowMap(), StreamFlowMapInner(), StreamFlowMapProps, streams, StreamFlowNode, StreamFlowNodeBody(), StreamNodeData (+14 more)

### Community 17 - "Community 17"
Cohesion: 0.06
Nodes (30): ApiFlow, ApiSource, ControllerService, ControllerServiceConfig, ControllerServiceDescriptor, ControllerServicesListResponse, ControllerServiceUpdateResult, FlowCreatePayload (+22 more)

### Community 18 - "Community 18"
Cohesion: 0.12
Nodes (25): ADDABLE, DedupFields(), defaultConfig(), KIND_LABEL, roundDisplay(), TransformsEditor(), UNIT_LABEL, UNIT_TO_HOURS (+17 more)

### Community 19 - "Community 19"
Cohesion: 0.11
Nodes (24): BlockFormProps, CeremonyDialogProps, BlockNodeData, OpenApiPanelProps, SinkConfigEditorProps, TestPanelProps, TransformsEditorProps, conditionFrom() (+16 more)

### Community 20 - "Community 20"
Cohesion: 0.08
Nodes (24): api, ApiError, BASE, getApiBase(), normalizeErrorDetail(), request(), IcebergSink, IcebergSinkListResponse (+16 more)

### Community 21 - "Community 21"
Cohesion: 0.12
Nodes (24): Toast, ToastAction, ToastActionElement, ToastClose, ToastDescription, ToastProps, ToastTitle, toastVariants (+16 more)

### Community 22 - "Community 22"
Cohesion: 0.13
Nodes (21): AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter(), AlertDialogHeader(), AlertDialogOverlay, AlertDialogTitle (+13 more)

### Community 23 - "Community 23"
Cohesion: 0.12
Nodes (17): formatDedupWindow(), HttpSettings(), METHODS_FOR_MODE(), relativeTime(), Section(), ServiceMode, FactRow(), Field() (+9 more)

### Community 24 - "Community 24"
Cohesion: 0.20
Nodes (19): FlowSettingsForm(), FlowSettingsFormProps, NewFlowPanel(), baseTopicName(), branchPathLabels(), cleanTopicOverride(), CRON_PRESETS, cronPreview() (+11 more)

### Community 25 - "Community 25"
Cohesion: 0.22
Nodes (20): BlockForm(), hostsTest(), noTestReason(), BranchesCard(), BranchesCardProps, BlockNode(), BRANCH_OPS, branchesOf() (+12 more)

### Community 26 - "Community 26"
Cohesion: 0.15
Nodes (17): ServiceSelector(), boolish(), buildConfig(), emptyForm(), formFromService(), numStr(), ProxyField(), saveBlockReason() (+9 more)

### Community 27 - "Community 27"
Cohesion: 0.15
Nodes (18): OpenApiPanel(), OpenApiPathCombobox(), OpenApiPathComboboxProps, PopoverContent, BASE, getOpenApiSpec(), listOpenApiOperations(), normalizeDetail() (+10 more)

### Community 28 - "Community 28"
Cohesion: 0.13
Nodes (18): dotClasses, isLive(), softClasses, StatusBadge(), Variant, variantOf, Table, TableBody (+10 more)

### Community 29 - "Community 29"
Cohesion: 0.09
Nodes (22): BlockMetric, BranchInfo, CeremonyDraft, ConnectConnectorRuntime, ConnectionHealth, ConnectorExport, ConnectRunState, ConnectTaskRuntime (+14 more)

### Community 30 - "Community 30"
Cohesion: 0.20
Nodes (15): DestinationsPanel(), Badge, BadgeProps, badgeVariants, Card, CardContent, CardDescription, CardFooter (+7 more)

### Community 31 - "Community 31"
Cohesion: 0.15
Nodes (20): appendJsonPathSegment(), asRecord(), buildResponseExplorerNode(), buildResponseInsights(), collectArraySuggestions(), collectFieldSuggestions(), dedupeFieldSuggestions(), formatKeyValueLines() (+12 more)

### Community 32 - "Community 32"
Cohesion: 0.15
Nodes (15): ApiSchemaArtifact, ApiSchemaVersion, GenerateResponse, SchemaArtifactCreatePayload, SchemaInferPayload, SchemaInferResponse, schemasApi, SchemaVersionStatus (+7 more)

### Community 33 - "Community 33"
Cohesion: 0.21
Nodes (16): TestPanel(), timeAgo(), AttentionRow, computeAttention(), Dashboard(), rootBlockOf(), statusDotClass(), getDashboardSummary() (+8 more)

### Community 34 - "Community 34"
Cohesion: 0.25
Nodes (17): asString(), createDefaultStream(), createExtractionRule(), createParamBinding(), createRoutingRule(), createTransformationRule(), initialPrimaryStream, makeId() (+9 more)

### Community 35 - "Community 35"
Cohesion: 0.20
Nodes (9): OPTIONS, HoverCardContent, Slider, ToggleGroup, ToggleGroupContext, ToggleGroupItem, Toggle, toggleVariants (+1 more)

### Community 36 - "Community 36"
Cohesion: 0.18
Nodes (12): ADAPTER_META, AdapterChip(), AddBlockMenu(), DropdownMenuCheckboxItem, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuRadioItem (+4 more)

### Community 37 - "Community 37"
Cohesion: 0.19
Nodes (13): Carousel, CarouselApi, CarouselContent, CarouselContext, CarouselContextProps, CarouselItem, CarouselNext, CarouselOptions (+5 more)

### Community 38 - "Community 38"
Cohesion: 0.16
Nodes (12): SheetContent, SheetContentProps, SheetDescription, SheetFooter(), SheetHeader(), SheetOverlay, SheetTitle, sheetVariants (+4 more)

### Community 39 - "Community 39"
Cohesion: 0.15
Nodes (12): auditLog, connections, ConnHealth, dashboardStats, flows, FlowState, flowSummary, rapid7AssetsAvro (+4 more)

### Community 40 - "Community 40"
Cohesion: 0.21
Nodes (10): METADATA_SOURCES, PAGINATION_TYPES, PaginationConfig, PaginationFields(), PaginationFieldsProps, STOP_CONDITIONS, SectionLabel(), SelectContent (+2 more)

### Community 41 - "Community 41"
Cohesion: 0.23
Nodes (10): FormControl, FormDescription, FormFieldContext, FormFieldContextValue, FormItem, FormItemContext, FormItemContextValue, FormLabel (+2 more)

### Community 42 - "Community 42"
Cohesion: 0.29
Nodes (10): filterFlowRows(), filterSchemaArtifacts(), includesQuery(), latestSchemaStatus(), matchesSchemaStatusFilter(), normalizeQuery(), SchemaArtifactStatusFilter, SearchableFlow (+2 more)

### Community 43 - "Community 43"
Cohesion: 0.25
Nodes (9): ChartConfig, ChartContainer, ChartContext, ChartContextProps, ChartLegendContent, ChartTooltipContent, getPayloadConfigFromPayload(), THEMES (+1 more)

### Community 44 - "Community 44"
Cohesion: 0.33
Nodes (9): AttributeExtractionLike, collectBranchStreamIds(), collectMissingPathParamResolutionsForInference(), InferencePreflightIssue, InferencePreflightStream, parentStreamIdFor(), RouteSourceLike, toAttributeName() (+1 more)

### Community 45 - "Community 45"
Cohesion: 0.27
Nodes (6): App(), queryClient, Toaster(), ToasterProps, Connections(), NotFound()

### Community 46 - "Community 46"
Cohesion: 0.27
Nodes (8): asString(), FlowDesignerConnectionPayload, FlowDesignerConnectionState, FlowDesignerSourceType, mergeSourceConnectionState(), normalizeMode(), SourceConnectionMode, SourceConnectionRecord

### Community 47 - "Community 47"
Cohesion: 0.25
Nodes (6): FlowImportCredentialValues, getNifiImportCredentialFields(), getSuggestedImportFlowName(), isFlowImportReady(), ImportQueryClient, refreshImportedFlowQueries()

### Community 48 - "Community 48"
Cohesion: 0.25
Nodes (6): DrawerContent, DrawerDescription, DrawerFooter(), DrawerHeader(), DrawerOverlay, DrawerTitle

### Community 49 - "Community 49"
Cohesion: 0.29
Nodes (7): NavigationMenu, NavigationMenuContent, NavigationMenuIndicator, NavigationMenuList, NavigationMenuTrigger, navigationMenuTriggerStyle, NavigationMenuViewport

### Community 50 - "Community 50"
Cohesion: 0.46
Nodes (6): FlowDesignerSchemaSourceType, FlowDesignerSchemaStatus, getEntitySchemaStatusLabel(), normalizeSourceType(), requiresSchemaWorkflow(), shouldBlockSaveForUnverifiedSchemas()

### Community 51 - "Community 51"
Cohesion: 0.40
Nodes (4): InputOTP, InputOTPGroup, InputOTPSeparator, InputOTPSlot

### Community 52 - "Community 52"
Cohesion: 0.40
Nodes (4): createImportCredentialValues(), FlowImportCredentialValidation, isImportCredentialValuesComplete(), summarizeImportCredentialValidation()

### Community 53 - "Community 53"
Cohesion: 0.60
Nodes (3): getVisibleSelectionState(), toggleVisibleSelection(), VisibleSelectionState

### Community 54 - "Community 54"
Cohesion: 0.50
Nodes (5): parseXPathSegments(), stripXPathIndex(), suggestXmlSplitFromNode(), toNamespaceSafeXPath(), toXmlExtractionPath()

### Community 58 - "Community 58"
Cohesion: 0.67
Nodes (3): createImportSchemaResolutions(), schemaNamespaceFromFlowName(), suggestedRenamedSchemaArtifactId()

## Knowledge Gaps
- **330 isolated node(s):** `queryClient`, `Props`, `mainItems`, `systemItems`, `NavLinkCompatProps` (+325 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **6 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `cn()` connect `Community 3` to `Community 0`, `Community 1`, `Community 2`, `Community 4`, `Community 6`, `Community 7`, `Community 9`, `Community 10`, `Community 11`, `Community 12`, `Community 14`, `Community 15`, `Community 16`, `Community 21`, `Community 22`, `Community 23`, `Community 25`, `Community 27`, `Community 28`, `Community 30`, `Community 33`, `Community 35`, `Community 36`, `Community 37`, `Community 38`, `Community 40`, `Community 41`, `Community 43`, `Community 48`, `Community 49`, `Community 51`?**
  _High betweenness centrality (0.283) - this node is a cross-community bridge._
- **Why does `Button` connect `Community 2` to `Community 0`, `Community 1`, `Community 3`, `Community 4`, `Community 6`, `Community 9`, `Community 10`, `Community 11`, `Community 14`, `Community 15`, `Community 18`, `Community 22`, `Community 23`, `Community 25`, `Community 27`, `Community 28`, `Community 30`, `Community 33`, `Community 37`?**
  _High betweenness centrality (0.018) - this node is a cross-community bridge._
- **Why does `Flow` connect `Community 19` to `Community 33`, `Community 2`, `Community 5`, `Community 6`, `Community 38`, `Community 9`, `Community 13`, `Community 14`, `Community 15`, `Community 18`, `Community 23`, `Community 24`, `Community 25`, `Community 29`, `Community 30`?**
  _High betweenness centrality (0.017) - this node is a cross-community bridge._
- **What connects `queryClient`, `Props`, `mainItems` to the rest of the system?**
  _330 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.02397003745318352 - nodes in this community are weakly interconnected._
- **Should `Community 1` be split into smaller, more focused modules?**
  _Cohesion score 0.05903866248693835 - nodes in this community are weakly interconnected._
- **Should `Community 2` be split into smaller, more focused modules?**
  _Cohesion score 0.060882800608828 - nodes in this community are weakly interconnected._