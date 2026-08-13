"""Tests for backend/services/adapter -- the Python port of the frontend's
flow-model rule engines (frontend/src/prototype/{legality,validation,naming}.ts).

Ports the representative cases from:
  - frontend/src/prototype/legality.test.ts
  - frontend/src/prototype/naming.test.ts
plus new validation cases the task brief asked for (dedup TTL bounds / single
dedup / identity fields are not in validation.ts today -- see
services/adapter/validation.py's module docstring) and a fully-valid flow
modeled on seeds.ts's "flow-rapid7".
"""

from pathlib import Path
import sys

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.adapter import (  # noqa: E402
    AppService,
    ApprovedSchema,
    Flow,
    FlowBlock,
    FlowTopic,
    TransformRule,
)
from services.adapter import legality, naming, validation  # noqa: E402


# --------------------------------------------------------------------------
# Builders (mirror legality.test.ts / naming.test.ts's `block`/`topic`/`flow`).
# --------------------------------------------------------------------------


def make_block(**over) -> FlowBlock:
    defaults = dict(
        id="b1",
        adapter="http",
        mode="read",
        name="Read",
        parentId=None,
        serviceId=None,
        entity=None,
        config={},
        transforms=[],
    )
    defaults.update(over)
    return FlowBlock(**defaults)


def make_topic(**over) -> FlowTopic:
    defaults = dict(id="t1", kind="materialized", name="raw.x.y", sealed=False)
    defaults.update(over)
    return FlowTopic(**defaults)


def make_flow(blocks=None, topics=None, name="Test Flow", **over) -> Flow:
    defaults = dict(
        id="f1",
        name=name,
        state="Draft",
        enabled=False,
        cron=None,
        blocks=blocks or [],
        topics=topics or [],
        variables=[],
        servicePins={},
        createdAt="",
        updatedAt="",
    )
    defaults.update(over)
    return Flow(**defaults)


# ==========================================================================
# naming.py
# ==========================================================================


class TestTokenize:
    def test_lowercases_and_collapses_non_alphanumerics(self):
        assert naming.tokenize("Rapid7  Assets!") == "rapid7_assets"
        assert naming.tokenize("  FortiSIEM--Events ") == "fortisiem_events"

    def test_consecutive_underscore_preservation(self):
        # "_" is itself non-alphanumeric, so a run of literal underscores (or
        # underscores mixed with other separators) collapses to exactly one
        # underscore -- there is never a `__` left to "preserve" separately;
        # this is what makes the trailing `_{2,}` collapse step in naming.ts
        # a no-op (see naming.py's tokenize docstring).
        assert naming.tokenize("a__b") == "a_b"
        assert naming.tokenize("a_-_b") == "a_b"
        assert naming.tokenize("__leading and trailing__") == "leading_and_trailing"
        assert naming.tokenize("multiple   spaces") == "multiple_spaces"


class TestDerivedNames:
    def test_derives_topic_table_dlq_names(self):
        assert naming.base_topic_name("Rapid7 Assets", "asset") == "raw.rapid7_assets.asset"
        assert naming.table_name("Rapid7 Assets", "asset") == "bronze.rapid7_assets.asset__raw"
        assert naming.dlq_name("Rapid7 Assets") == "dlq.rapid7_assets"

    def test_sole_writer_gets_bare_name(self):
        b = make_block(id="b1", adapter="kafka", mode="write", entity="asset")
        f = make_flow([b], name="Asset Retirement")
        assert naming.derive_topic_name(f, b).value == "raw.asset_retirement.asset"

    def test_governed_write_wins_bare_name_schemaless_copy_takes_branch_variant(self):
        governed = make_block(id="g", adapter="kafka_kc", mode=None, entity="asset", branch={"name": "all"})
        copy = make_block(id="c", adapter="kafka", mode="write", entity="asset", branch={"name": "active"})
        f = make_flow([governed, copy], name="Asset Retirement")
        assert naming.derive_topic_name(f, governed).value == "raw.asset_retirement.asset"
        assert naming.derive_topic_name(f, copy).value == "raw.asset_retirement.asset.active"

    def test_honors_custom_override_and_warns_on_raw_namespace(self):
        b = make_block(id="b1", adapter="kafka", mode="write", entity="asset", topicOverride="asset_retired")
        f = make_flow([b], name="Asset Retirement")
        d = naming.derive_topic_name(f, b)
        assert d.value == "asset_retired"
        assert d.overridden is True
        assert d.warning is None

        raw_b = make_block(id="b1", adapter="kafka", mode="write", entity="asset", topicOverride="raw.sneaky.name")
        assert naming.derive_topic_name(make_flow([raw_b]), raw_b).warning is not None
        assert "raw.*" in naming.derive_topic_name(make_flow([raw_b]), raw_b).warning

    def test_override_honored_on_kafka_kc_too(self):
        governed = make_block(id="g", adapter="kafka_kc", mode=None, entity="asset", topicOverride="Governed Feed!")
        f = make_flow([governed], name="Asset Retirement")
        d = naming.derive_topic_name(f, governed)
        assert d.value == "governed_feed_"
        assert d.overridden is True
        assert naming.clean_topic_override("Governed Feed!") == "governed_feed_"

    def test_override_ignored_on_blocks_with_no_topic(self):
        read = make_block(id="r", adapter="http", mode="read", topicOverride="nope")
        assert naming.derive_topic_name(make_flow([read]), read).value == ""

    def test_derived_default_and_override_match(self):
        b = make_block(id="b1", adapter="kafka", mode="write", entity="asset", topicOverride="raw.asset_retirement.asset")
        f = make_flow([b], name="Asset Retirement")
        assert naming.derived_topic_default(f, b).value == "raw.asset_retirement.asset"
        assert naming.override_matches_derived(f, b) is True

        custom = make_block(id="b1", adapter="kafka", mode="write", entity="asset", topicOverride="asset_retired")
        assert naming.override_matches_derived(make_flow([custom], name="Asset Retirement"), custom) is False

        bare = make_block(id="b1", adapter="kafka", mode="write", entity="asset")
        bare_flow = make_flow([bare], name="Asset Retirement")
        assert naming.override_matches_derived(bare_flow, bare, "RAW.Asset_Retirement.asset") is True
        assert naming.override_matches_derived(bare_flow, bare, "") is False

    def test_flags_reserved_name_collisions(self):
        assert naming.topic_name_collision("raw.rapid7_assets.asset") is not None
        assert naming.topic_name_collision("raw.brand_new.topic") is None


class TestCron:
    def test_validates_5_field_expressions_and_previews_presets(self):
        assert naming.is_valid_cron("*/15 * * * *") is True
        assert naming.is_valid_cron("15 * * *") is False
        assert naming.is_valid_cron(None) is True
        assert len(naming.cron_preview("*/15 * * * *")) == 3
        assert len(naming.cron_preview(None)) == 0


# ==========================================================================
# legality.py
# ==========================================================================


class TestRootMenu:
    def test_offers_legal_roots_and_refuses_kafka_kc(self):
        menu = legality.compute_root_menu()
        legal = [e.key for e in menu if not e.disabledReason]
        assert legal == ["http-read", "http-write", "jdbc-read", "kafka-read"]
        kkc = next(e for e in menu if e.key == "kafka_kc-root")
        assert "R2" in kkc.disabledReason
        assert any(e.futureScope for e in menu)


class TestAddMenu:
    def test_terminal_blocks_get_no_add_menu(self):
        kc = make_block(id="kc1", adapter="kc", mode=None)
        kkc = make_block(id="kkc1", adapter="kafka_kc", mode=None)
        assert legality.compute_add_menu(make_flow([kc]), "kc1") == []
        assert legality.compute_add_menu(make_flow([kkc]), "kkc1") == []

    def test_quarantines_raw_branches_r8(self):
        raw_read = make_block(id="r", adapter="kafka", mode="read", config={"parseFormat": "raw"})
        menu = legality.compute_add_menu(make_flow([raw_read]), "r")
        legal = sorted(e.key for e in menu if not e.disabledReason)
        assert legal == ["kafka-write"]
        kkc = next(e for e in menu if e.key == "kafka_kc")
        assert "R8" in kkc.disabledReason

    def test_raw_status_propagates_down_branch(self):
        raw_read = make_block(id="r", adapter="kafka", mode="read", config={"parseFormat": "raw"})
        child = make_block(id="w", adapter="kafka", mode="write", parentId="r")
        assert legality.is_raw_branch(make_flow([raw_read, child]), child) is True


class TestTopicMenus:
    def test_sealed_topics_refuse_everything(self):
        t = make_topic(sealed=True)
        entries = legality.compute_topic_menu(make_flow([], [t]), "t1")
        assert all(e.disabledReason for e in entries)

    def test_unsealed_topics_offer_kafka_read_and_kc_only(self):
        t = make_topic()
        keys = [e.key for e in legality.compute_topic_menu(make_flow([], [t]), "t1")]
        assert keys == ["kafka-read", "kc"]


class TestSubtreeIds:
    def test_walks_blocks_and_materialized_topics(self):
        root = make_block(id="root")
        write = make_block(id="w", adapter="kafka", mode="write", parentId="root")
        t = make_topic(id="t-w", writerBlockId="w")
        sink = make_block(id="kc1", adapter="kc", mode=None, parentId="t-w")
        f = make_flow([root, write, sink], [t])
        assert sorted(legality.subtree_ids(f, "w")) == ["kc1", "t-w", "w"]
        assert legality.subtree_ids(f, "kc1") == ["kc1"]

    def test_terminates_on_cyclic_parent_chain(self):
        a = make_block(id="a", parentId="b")
        b = make_block(id="b", parentId="a")
        assert sorted(legality.subtree_ids(make_flow([a, b]), "a")) == ["a", "b"]


class TestCanReparent:
    def _build(self):
        root = make_block(id="root")
        mid = make_block(id="mid", parentId="root")
        leaf = make_block(id="leaf", parentId="mid")
        kkc = make_block(id="kkc", adapter="kafka_kc", mode=None, parentId="root")
        sealed = make_topic(id="t-sealed", sealed=True, writerBlockId="kkc")
        open_topic = make_topic(id="t-open", kind="adopted", name="partner.feed")
        return make_flow([root, mid, leaf, kkc], [sealed, open_topic])

    def test_refuses_root_self_and_subtree_loop(self):
        f = self._build()
        assert "root block cannot be re-parented" in legality.can_reparent(f, "root", "mid")
        assert "cannot be its own parent" in legality.can_reparent(f, "mid", "mid")
        assert "loop" in legality.can_reparent(f, "mid", "leaf")

    def test_allows_sideways_move_and_noop(self):
        f = self._build()
        assert legality.can_reparent(f, "leaf", "root") is None
        assert legality.can_reparent(f, "leaf", "mid") is None

    def test_applies_terminal_topic_and_kc_rules(self):
        f = self._build()
        assert "terminal" in legality.can_reparent(f, "leaf", "kkc")
        assert "R5" in legality.can_reparent(f, "leaf", "t-open")
        assert "Sealed" in legality.can_reparent(f, "leaf", "t-sealed")

        with_kc_blocks = list(self._build().blocks) + [
            make_block(id="kc1", adapter="kc", mode=None, parentId="t-open", config={"attachTopicId": "t-open"})
        ]
        with_kc = make_flow(with_kc_blocks, self._build().topics)
        assert "never to a block" in legality.can_reparent(with_kc, "kc1", "mid")
        assert "Sealed" in legality.can_reparent(with_kc, "kc1", "t-sealed")

    def test_quarantines_raw_branches_including_traveling_children(self):
        raw_read = make_block(id="raw", adapter="kafka", mode="read", config={"parseFormat": "raw"})
        root = make_block(id="root")
        http_child = make_block(id="h", parentId="root")
        kafka_write = make_block(id="kw", adapter="kafka", mode="write", parentId="root")
        enricher = make_block(id="e", mode="lookup", parentId="kw")
        f = make_flow([root, raw_read, http_child, kafka_write, enricher])
        assert "R8" in legality.can_reparent(f, "h", "raw")

        f2 = make_flow([root, raw_read, make_block(id="kw2", adapter="kafka", mode="write", parentId="root")])
        assert legality.can_reparent(f2, "kw2", "raw") is None

        assert "R8" in legality.can_reparent(f, "kw", "raw")


class TestTriggers:
    def test_http_jdbc_roots_scheduled_kafka_roots_continuous(self):
        assert legality.flow_has_trigger(make_flow([make_block()])) is True
        assert legality.flow_has_trigger(make_flow([make_block(adapter="jdbc")])) is True
        assert legality.flow_has_trigger(make_flow([make_block(adapter="kafka", config={"parseFormat": "json"})])) is False

    def test_finds_root_through_adopted_topic(self):
        t = make_topic(id="adopt", kind="adopted", name="partner.feed")
        read = make_block(id="r", adapter="kafka", mode="read", parentId="adopt")
        assert legality.root_block(make_flow([read], [t])).id == "r"


# ==========================================================================
# legality.validate_placement (new: whole-tree structural check)
# ==========================================================================


class TestValidatePlacement:
    def test_terminal_parent_rejected(self):
        root = make_block(id="root")
        kkc = make_block(id="kkc", adapter="kafka_kc", mode=None, parentId="root")
        stray = make_block(id="stray", parentId="kkc")  # illegal: parent is terminal
        f = make_flow([root, kkc, stray])
        violations = legality.validate_placement(f)
        assert any(v.blockId == "stray" and "terminal" in v.message for v in violations)

    def test_kafka_kc_as_root_rejected(self):
        kkc = make_block(id="kkc", adapter="kafka_kc", mode=None, parentId=None)
        f = make_flow([kkc])
        violations = legality.validate_placement(f)
        assert any(v.blockId == "kkc" and "R2" in v.message for v in violations)

    def test_kc_only_attaches_to_unsealed_topic(self):
        sealed = make_topic(id="t-sealed", sealed=True)
        kc = make_block(id="kc1", adapter="kc", mode=None, parentId="t-sealed")
        f = make_flow([kc], [sealed])
        violations = legality.validate_placement(f)
        assert any(v.blockId == "kc1" and "Sealed" in v.message for v in violations)

    def test_raw_branch_quarantine_no_transforms(self):
        raw_read = make_block(id="raw", adapter="kafka", mode="read", config={"parseFormat": "raw"})
        write = make_block(
            id="w",
            adapter="kafka",
            mode="write",
            parentId="raw",
            transforms=[TransformRule(id="t1", kind="add_field", config={"field": "x", "value": "y"})],
        )
        f = make_flow([raw_read, write])
        violations = legality.validate_placement(f)
        assert any(v.blockId == "w" and "R8" in v.message for v in violations)

    def test_cron_only_legal_on_http_jdbc_root(self):
        kafka_root = make_block(id="r", adapter="kafka", mode="read", config={"parseFormat": "json"})
        f = make_flow([kafka_root], cron="0 * * * *")
        violations = legality.validate_placement(f)
        assert any(v.blockId is None and "R1" in v.message for v in violations)

    def test_legal_tree_has_no_violations(self):
        root = make_block(id="root", adapter="http", mode="read")
        write = make_block(id="w", adapter="kafka", mode="write", parentId="root", entity="asset")
        f = make_flow([root, write])
        assert legality.validate_placement(f) == []


# ==========================================================================
# validation.py
# ==========================================================================


class TestValidateBlock:
    def test_terminal_parent_child_still_reports_via_placement_not_block(self):
        # validateBlock is per-block config sanity; the terminal-parent rule
        # itself lives in validate_placement (see TestValidatePlacement) --
        # this just confirms a plain, otherwise-valid child block hosted
        # under a terminal parent produces no *block-level* issue on its own.
        kkc = make_block(
            id="kkc", adapter="kafka_kc", mode=None, entity="asset", serviceId="svc-1", config={"sinkServiceId": "svc-1"}
        )
        schemas = [ApprovedSchema(id="s1", flowId="f1", blockId="kkc")]
        services = [AppService(id="svc-1", type="sink_destination", name="Sink", retired=False)]
        f = make_flow([kkc], name="F")
        issues = validation.validate_block(f, kkc, services, schemas)
        assert issues == []

    def test_write_without_entity_rejected(self):
        write = make_block(id="w", adapter="kafka", mode="write", entity=None)
        f = make_flow([write])
        issues = validation.validate_block(f, write, [], [])
        assert any("entity" in i.message for i in issues)

    def test_kc_write_without_entity_rejected(self):
        kc = make_block(id="kc1", adapter="kc", mode=None, entity=None, config={"attachTopicId": "t1"})
        f = make_flow([kc])
        issues = validation.validate_block(f, kc, [], [])
        assert any("entity" in i.message for i in issues)

    def test_dedup_not_last_rejected(self):
        b = make_block(
            id="w",
            adapter="kafka",
            mode="write",
            entity="asset",
            transforms=[
                TransformRule(id="t1", kind="dedup", config={"identityFields": ["id"], "windowHours": 24}),
                TransformRule(id="t2", kind="remove_field", config={"field": "x"}),
            ],
        )
        f = make_flow([b])
        issues = validation.validate_block(f, b, [], [])
        assert any("Dedup must be the last transformation" in i.message for i in issues)

    def test_single_dedup_enforced(self):
        b = make_block(
            id="w",
            adapter="kafka",
            mode="write",
            entity="asset",
            transforms=[
                TransformRule(id="t1", kind="dedup", config={"identityFields": ["id"], "windowHours": 24}),
                TransformRule(id="t2", kind="dedup", config={"identityFields": ["id"], "windowHours": 12}),
            ],
        )
        f = make_flow([b])
        issues = validation.validate_block(f, b, [], [])
        assert any("Only one dedup" in i.message for i in issues)

    def test_dedup_ttl_bound_rejected(self):
        too_long = make_block(
            id="w1",
            adapter="kafka",
            mode="write",
            entity="asset",
            transforms=[TransformRule(id="t1", kind="dedup", config={"identityFields": ["id"], "windowHours": 9000})],
        )
        too_short = make_block(
            id="w2",
            adapter="kafka",
            mode="write",
            entity="asset",
            transforms=[TransformRule(id="t1", kind="dedup", config={"identityFields": ["id"], "windowHours": 0})],
        )
        f = make_flow([too_long, too_short])
        issues_long = validation.validate_block(f, too_long, [], [])
        issues_short = validation.validate_block(f, too_short, [], [])
        assert any("Dedup window must be between" in i.message for i in issues_long)
        assert any("Dedup window must be between" in i.message for i in issues_short)

    def test_dedup_needs_identity_field(self):
        b = make_block(
            id="w",
            adapter="kafka",
            mode="write",
            entity="asset",
            transforms=[TransformRule(id="t1", kind="dedup", config={"identityFields": [], "windowHours": 24})],
        )
        f = make_flow([b])
        issues = validation.validate_block(f, b, [], [])
        assert any("at least one identity field" in i.message for i in issues)

    def test_dedup_default_window_and_valid_identity_passes(self):
        b = make_block(
            id="w",
            adapter="kafka",
            mode="write",
            entity="asset",
            transforms=[TransformRule(id="t1", kind="dedup", config={"identityFields": ["id"]})],
        )
        f = make_flow([b])
        issues = validation.validate_block(f, b, [], [])
        assert not any("Dedup window" in i.message for i in issues)

    def test_raw_branch_transform_rejected(self):
        raw_read = make_block(id="raw", adapter="kafka", mode="read", config={"parseFormat": "raw"})
        write = make_block(
            id="w",
            adapter="kafka",
            mode="write",
            parentId="raw",
            entity="asset",
            transforms=[TransformRule(id="t1", kind="add_field", config={"field": "x", "value": "y"})],
        )
        f = make_flow([raw_read, write])
        issues = validation.validate_block(f, write, [], [])
        assert any("R8" in i.message for i in issues)


class TestValidateFlow:
    def test_kafka_kc_as_root_rejected_by_placement(self):
        # rootBlock() is purely structural -- `flow.blocks.find(b => b.parentId
        # === null)` -- it does not itself check R2, so a bare kafka_kc IS
        # picked up as "the root" and validateFlow's "no legal root" message
        # does not fire for it (this matches naming.ts/legality.ts's actual
        # behavior exactly: R2 is enforced by computeRootMenu/canReparent
        # refusing to ever *construct* such a tree, not by rootBlock()
        # rejecting one that already exists). validate_placement() is what
        # catches an already-persisted illegal bare kafka_kc root.
        kkc = make_block(
            id="kkc", adapter="kafka_kc", mode=None, entity="asset", serviceId="svc-1", config={"sinkServiceId": "svc-1"}
        )
        f = make_flow([kkc], name="F")
        assert legality.root_block(f) is kkc  # structural: bare block, adapter not checked

        issues = validation.validate_flow(f, [], [ApprovedSchema(id="s1", flowId="f1", blockId="kkc")])
        assert not any("no legal root" in i.message for i in issues)

        violations = legality.validate_placement(f)
        assert any(v.blockId == "kkc" and "R2" in v.message for v in violations)

    def test_fully_valid_flow_passes(self):
        # Modeled on seeds.ts's "flow-rapid7": http·read root -> kafka_kc
        # governed write, with a service for each block, an approved schema
        # for the governed write, and a valid cron on the http root.
        http_svc = AppService(id="svc-http", type="http", name="Rapid7 API", retired=False, health="Healthy")
        sink_svc = AppService(id="svc-iceberg", type="sink_destination", name="Iceberg", retired=False, health="Healthy")

        root = FlowBlock(
            id="b-list",
            adapter="http",
            mode="read",
            name="List Assets",
            parentId=None,
            serviceId="svc-http",
            config={"method": "GET", "path": "/api/3/assets"},
            transforms=[],
        )
        sink = FlowBlock(
            id="b-sink",
            adapter="kafka_kc",
            name="Assets to Iceberg",
            parentId="b-list",
            serviceId="svc-iceberg",
            entity="asset",
            config={"sinkServiceId": "svc-iceberg"},
            transforms=[],
        )
        topic = FlowTopic(id="t-sink", kind="materialized", name="raw.rapid7_assets.asset", sealed=True, writerBlockId="b-sink")
        schema = ApprovedSchema(id="schema-1", flowId="flow-rapid7", blockId="b-sink")

        f = Flow(
            id="flow-rapid7",
            name="Rapid7 Assets",
            state="Draft",
            enabled=False,
            cron="0 */6 * * *",
            blocks=[root, sink],
            topics=[topic],
            variables=[],
            servicePins={"svc-http": 1, "svc-iceberg": 1},
            createdAt="",
            updatedAt="",
        )

        issues = validation.validate_flow(f, [http_svc, sink_svc], [schema])
        assert issues == []
        assert legality.validate_placement(f) == []


def test_import_smoke():
    """Mirrors the task's `python -c "import services.adapter..."` smoke check."""
    import services.adapter.common  # noqa: F401
    import services.adapter.legality  # noqa: F401
    import services.adapter.naming  # noqa: F401
    import services.adapter.validation  # noqa: F401

def test_http_full_url_in_path_rejected():
    """Backend mirror of frontend httpPathIssue(): a full URL typed into the
    http path field compiles to base+url concatenation and an invalid
    InvokeHTTP target (user-reported live failure) — must be an issue."""
    http_svc = AppService(id="svc-http", type="http", name="API", retired=False, health="Healthy")
    root = FlowBlock(
        id="b-r", adapter="http", mode="read", name="Read", parentId=None,
        serviceId="svc-http",
        config={"method": "GET", "path": "https://dummyjson.com/users"},
        transforms=[],
    )
    out = FlowBlock(
        id="b-w", adapter="kafka", mode="write", name="Out", parentId="b-r",
        entity="thing", config={}, transforms=[],
    )
    f = Flow(id="f-url", name="Url Flow", state="Draft", enabled=True,
             cron="0 12 * * *", blocks=[root, out], topics=[], variables=[],
             servicePins={}, createdAt="", updatedAt="")
    issues = validation.validate_flow(f, [http_svc], [])
    assert any("got a full URL" in i.message for i in issues), [i.message for i in issues]
