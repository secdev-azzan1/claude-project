from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid
import re


class StreamPagination(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    type: str = "none"  # none | page | cursor | offset | next_url
    page_param: Optional[str] = None
    page_size_param: Optional[str] = None
    page_size: Optional[int] = None
    first_page_number: Optional[int] = None
    max_pages: Optional[int] = None
    total_count_stop_rule: Optional[str] = None
    cursor_param: Optional[str] = None
    cursor_path: Optional[str] = None
    cursor_stop_path: Optional[str] = None
    cursor_header: Optional[str] = None
    offset_param: Optional[str] = None
    limit_param: Optional[str] = None
    offset_total_count_stop_rule: Optional[str] = None
    total_count_path: Optional[str] = None
    next_url_path: Optional[str] = None
    next_url_header: Optional[str] = None
    stop_condition: Optional[str] = None
    total_count_header: Optional[str] = None
    has_more_path: Optional[str] = None
    has_more_header: Optional[str] = None
    location: str = "query"  # query | body
    next_url_header_format: Optional[str] = None  # raw | link

    @field_validator("type")
    @classmethod
    def _validate_type(cls, value: str) -> str:
        text = (value or "none").strip().lower()
        aliases = {
            "none": "none",
            "page": "page",
            "page_increment": "page",
            "page increment": "page",
            "cursor": "cursor",
            "cursor_pagination": "cursor",
            "offset": "offset",
            "offset_pagination": "offset",
            "next_url": "next_url",
            "next-url": "next_url",
            "next": "next_url",
            "next_url_pagination": "next_url",
        }
        if text not in aliases:
            raise ValueError("pagination type must be one of: none, page, cursor, offset, next_url")
        return aliases[text]

    @model_validator(mode="after")
    def _validate_enabled_type(self):
        if self.enabled and self.type == "none":
            raise ValueError("pagination type cannot be 'none' when pagination is enabled")
        return self

    @field_validator("location")
    @classmethod
    def _validate_location(cls, value: str) -> str:
        text = (value or "query").strip().lower()
        if text not in {"query", "body"}:
            raise ValueError("pagination location must be one of: query, body")
        return text

    @field_validator("next_url_header_format")
    @classmethod
    def _validate_next_url_header_format(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return None
        text = value.strip().lower()
        if text not in {"raw", "link"}:
            raise ValueError("next_url_header_format must be one of: raw, link")
        return text

    @field_validator("max_pages")
    @classmethod
    def _positive_optional_int(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and value < 1:
            raise ValueError("must be greater than or equal to 1")
        return value

    @field_validator("first_page_number")
    @classmethod
    def _non_negative_optional_int(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and value < 0:
            raise ValueError("must be greater than or equal to 0")
        return value


class StreamPolling(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    status_path: Optional[str] = None
    pending_values: List[str] = Field(default_factory=list)
    success_values: List[str] = Field(default_factory=list)
    failure_values: List[str] = Field(default_factory=list)
    interval_seconds: Optional[int] = None
    max_attempts: Optional[int] = None
    missing_status_behavior: str = "fail"

    @field_validator("interval_seconds", "max_attempts")
    @classmethod
    def _positive_optional_int(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and value < 1:
            raise ValueError("must be greater than or equal to 1")
        return value

    @field_validator("missing_status_behavior")
    @classmethod
    def _validate_missing_status_behavior(cls, value: str) -> str:
        text = (value or "fail").strip().lower()
        if text not in {"fail", "treat_as_pending"}:
            raise ValueError("missing_status_behavior must be one of: fail, treat_as_pending")
        return text


class StreamFanOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    parent_stream_id: Optional[str] = None
    completion_mode: str = "per_record"
    parent_field: Optional[str] = None
    inject_as: Optional[str] = None  # query_param | path_param | header | body
    inject_field_name: Optional[str] = None
    inject_template: Optional[str] = None

    @field_validator("completion_mode")
    @classmethod
    def _validate_completion_mode(cls, value: str) -> str:
        text = (value or "per_record").strip().lower()
        aliases = {
            "per_record": "per_record",
            "per parent record": "per_record",
            "each_record": "per_record",
            "each record": "per_record",
            "after_all": "after_all",
            "after all": "after_all",
            "after_all_records": "after_all",
            "after all records": "after_all",
        }
        if text not in aliases:
            raise ValueError("completion_mode must be one of: per_record, after_all")
        return aliases[text]

    @field_validator("inject_as")
    @classmethod
    def _validate_inject_as(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return None
        text = value.strip().lower()
        if text not in {"query_param", "path_param", "header", "body"}:
            raise ValueError("inject_as must be one of: query_param, path_param, header, body")
        return text


class ExtractionRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    attribute_name: str
    path: str
    default_value: Optional[str] = None

    @field_validator("attribute_name", "path")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        text = (value or "").strip()
        if not text:
            raise ValueError("must not be empty")
        return text


class StreamParameterBinding(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: Optional[str] = None
    param_name: str
    param_in: str  # path | query | header | body
    required: bool = False
    enabled: bool = False
    value_source: str = "manual_runtime"  # parent_field | static | global_var | manual_runtime
    parent_stream_id: Optional[str] = None
    parent_field: Optional[str] = None
    static_value: Optional[str] = None
    global_var_name: Optional[str] = None
    runtime_var_name: Optional[str] = None
    confidence: Optional[str] = None  # high | medium | low
    description: Optional[str] = None
    schema_type: Optional[str] = None
    default_value: Optional[str] = None

    @field_validator("param_name")
    @classmethod
    def _validate_param_name(cls, value: str) -> str:
        text = (value or "").strip()
        if not text:
            raise ValueError("param_name must not be empty")
        return text

    @field_validator("param_in")
    @classmethod
    def _validate_param_in(cls, value: str) -> str:
        text = (value or "").strip().lower()
        if text not in {"path", "query", "header", "body"}:
            raise ValueError("param_in must be one of: path, query, header, body")
        return text

    @field_validator("value_source")
    @classmethod
    def _validate_value_source(cls, value: str) -> str:
        text = (value or "manual_runtime").strip().lower()
        if text not in {"parent_field", "static", "global_var", "manual_runtime"}:
            raise ValueError("value_source must be one of: parent_field, static, global_var, manual_runtime")
        return text

    @field_validator("confidence")
    @classmethod
    def _validate_confidence(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return None
        text = value.strip().lower()
        if text not in {"high", "medium", "low"}:
            raise ValueError("confidence must be one of: high, medium, low")
        return text


class TransformationRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str  # add_field | remove_field | set_from_attribute
    field_name: str
    value: Optional[str] = None
    source_attribute: Optional[str] = None

    @field_validator("type")
    @classmethod
    def _validate_type(cls, value: str) -> str:
        text = (value or "").strip().lower()
        if text not in {"add_field", "remove_field", "set_from_attribute"}:
            raise ValueError("transformation type must be one of: add_field, remove_field, set_from_attribute")
        return text


class RoutingRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    attribute: str
    condition: str  # equals | contains | starts_with | regex
    value: str
    destination: str
    topic_name: Optional[str] = None

    @field_validator("condition")
    @classmethod
    def _validate_condition(cls, value: str) -> str:
        text = (value or "").strip().lower()
        if text not in {"equals", "contains", "starts_with", "ends_with", "regex", "is_empty"}:
            raise ValueError("routing condition must be one of: equals, contains, starts_with, ends_with, regex, is_empty")
        return text

    @field_validator("destination")
    @classmethod
    def _validate_destination(cls, value: str) -> str:
        text = (value or "").strip().lower()
        if text not in {"include", "exclude", "route_to_topic"}:
            raise ValueError("routing destination must be one of: include, exclude, route_to_topic")
        return text

    @model_validator(mode="after")
    def _validate_topic_destination(self):
        if self.destination == "route_to_topic" and not (self.topic_name or "").strip():
            raise ValueError("topic_name is required when routing destination is route_to_topic")
        return self


class Stream(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    endpoint_path: str = "/"
    request_enabled: Optional[bool] = None
    method: str = "GET"
    response_format: str = "json"  # json | xml | csv
    response_data_path: Optional[str] = None
    split_response_records: Optional[bool] = None
    wait_for_all_records_before_downstream: bool = False
    xml_split_depth: Optional[int] = None  # for SplitXml depth (default 1)
    xml_record_element: Optional[str] = None
    csv_delimiter: Optional[str] = ","
    csv_has_header_row: Optional[bool] = True
    csv_quote_char: Optional[str] = '"'
    csv_escape_char: Optional[str] = "\\"
    csv_encoding: Optional[str] = "utf-8"
    csv_test_record_limit: Optional[int] = 50
    # MongoDB stream-level query config (preferred over source-level db_* for Mongo streams)
    mongo_query: Optional[str] = None
    mongo_projection: Optional[str] = None
    mongo_sort: Optional[str] = None
    mongo_limit: Optional[int] = None
    primary_key_fields: List[str] = Field(default_factory=list)
    is_primary: bool = False
    headers: Dict[str, str] = Field(default_factory=dict)
    query_params: Dict[str, str] = Field(default_factory=dict)
    body_format: str = "empty"  # empty | json | xml | csv
    body_template: Optional[str] = None
    path_param_defaults: Dict[str, str] = Field(default_factory=dict)
    pagination: StreamPagination = Field(default_factory=StreamPagination)
    polling: StreamPolling = Field(default_factory=StreamPolling)
    fan_out: StreamFanOut = Field(default_factory=StreamFanOut)
    extraction_rules: List[ExtractionRule] = Field(default_factory=list)
    transformation_rules: List[TransformationRule] = Field(default_factory=list)
    routing_rules: List[RoutingRule] = Field(default_factory=list)
    openapi_operation_id: Optional[str] = None
    parameter_bindings: List[StreamParameterBinding] = Field(default_factory=list)
    # New routing/entity fields (Phase 4 routing model)
    # Using Optional[Any] with Dict to avoid forward-ref issues; validated at write time
    filters: List[Dict[str, Any]] = Field(default_factory=list)
    routes: List[Dict[str, Any]] = Field(default_factory=list)
    default_route_child_stream_id: Optional[str] = None
    default_route_action: Optional[str] = None
    route_source: Optional[Dict[str, Any]] = None
    entity: Optional[Dict[str, Any]] = None

    @field_validator("name")
    @classmethod
    def _non_empty_name(cls, value: str) -> str:
        text = (value or "").strip()
        if not text:
            raise ValueError("stream name must not be empty")
        return text

    @field_validator("method")
    @classmethod
    def _validate_method(cls, value: str) -> str:
        text = (value or "GET").strip().upper()
        if text not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
            raise ValueError("method must be one of: GET, POST, PUT, PATCH, DELETE")
        return text

    @field_validator("response_format")
    @classmethod
    def _validate_response_format(cls, value: str) -> str:
        text = (value or "json").strip().lower()
        if text not in {"json", "xml", "csv"}:
            raise ValueError("response_format must be one of: json, xml, csv")
        return text

    @field_validator("body_format")
    @classmethod
    def _validate_body_format(cls, value: str) -> str:
        text = (value or "empty").strip().lower()
        if text not in {"empty", "json", "xml", "csv"}:
            raise ValueError("body_format must be one of: empty, json, xml, csv")
        return text

    @field_validator("xml_split_depth", "mongo_limit", "csv_test_record_limit")
    @classmethod
    def _positive_optional_int(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and value < 1:
            raise ValueError("must be greater than or equal to 1")
        return value

    @field_validator("csv_delimiter", "csv_quote_char", "csv_escape_char")
    @classmethod
    def _single_char_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        text = str(value)
        if text == "":
            return None
        if len(text) != 1:
            raise ValueError("must be a single character")
        return text

    @field_validator("csv_encoding")
    @classmethod
    def _normalize_csv_encoding(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        text = str(value).strip()
        if not text:
            return "utf-8"
        return text


class SourceSchedule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = "interval"  # interval | cron
    interval_value: int = 15
    interval_unit: str = "minutes"  # seconds | minutes | hours | days
    cron_expression: Optional[str] = None

    @field_validator("type")
    @classmethod
    def _validate_type(cls, value: str) -> str:
        text = (value or "interval").strip().lower()
        if text not in {"interval", "cron"}:
            raise ValueError("schedule type must be one of: interval, cron")
        return text

    @field_validator("interval_unit")
    @classmethod
    def _validate_unit(cls, value: str) -> str:
        text = (value or "minutes").strip().lower()
        if text not in {"seconds", "minutes", "hours", "days"}:
            raise ValueError("interval_unit must be one of: seconds, minutes, hours, days")
        return text

    @field_validator("interval_value")
    @classmethod
    def _validate_interval(cls, value: int) -> int:
        if value < 1:
            raise ValueError("interval_value must be greater than or equal to 1")
        return value


class KafkaOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str = ""
    partitions: int = 6
    replication_factor: int = 3
    key_strategy: str = "none"  # primary_key | none
    value_format: str = "avro"  # avro | json
    schema_mode: str = "existing"  # existing | auto_generate
    schema_artifact_id: Optional[str] = None
    schema_version: Optional[int] = None

    @field_validator("key_strategy")
    @classmethod
    def _validate_key_strategy(cls, value: str) -> str:
        text = (value or "none").strip().lower()
        if text not in {"primary_key", "none"}:
            raise ValueError("key_strategy must be one of: primary_key, none")
        return text

    @field_validator("value_format")
    @classmethod
    def _validate_value_format(cls, value: str) -> str:
        text = (value or "avro").strip().lower()
        if text not in {"avro"}:
            raise ValueError("value_format must be avro")
        return text

    @field_validator("schema_mode")
    @classmethod
    def _validate_schema_mode(cls, value: str) -> str:
        text = (value or "existing").strip().lower()
        if text not in {"existing", "auto_generate"}:
            raise ValueError("schema_mode must be one of: existing, auto_generate")
        return text


# ---------------------------------------------------------------------------
# New routing model classes (Phase 4)
# ---------------------------------------------------------------------------

class FilterRule(BaseModel):
    """Include/exclude filter on a stream (non-branching). Orthogonal to routes."""
    model_config = ConfigDict(extra="allow")

    id: Optional[str] = None
    attribute: str
    condition: str  # equals|contains|starts_with|ends_with|regex|is_empty
    value: str = ""
    action: str = "include"  # include|exclude

    @field_validator("condition")
    @classmethod
    def _validate_condition(cls, v):
        if v not in {"equals", "contains", "starts_with", "ends_with", "regex", "is_empty"}:
            raise ValueError("invalid condition")
        return v

    @field_validator("action")
    @classmethod
    def _validate_action(cls, v):
        if v not in {"include", "exclude"}:
            raise ValueError("action must be include or exclude")
        return v


class RouteCondition(BaseModel):
    model_config = ConfigDict(extra="allow")

    attribute: str
    condition: str  # equals|contains|starts_with|ends_with|regex|is_empty
    value: str = ""

    @field_validator("condition")
    @classmethod
    def _validate_condition(cls, v):
        if v not in {"equals", "contains", "starts_with", "ends_with", "regex", "is_empty"}:
            raise ValueError("invalid condition")
        return v


class Route(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    condition: RouteCondition
    child_stream_id: str
    ordinal: int = 0


class RouteSource(BaseModel):
    """Marks this stream as a route-child of another stream."""
    model_config = ConfigDict(extra="allow")

    parent_stream_id: str
    route_id: str


class EntityIcebergConfig(BaseModel):
    """Per-entity-stream intent to sync its Kafka topic into an Iceberg table."""
    model_config = ConfigDict(extra="ignore")

    enabled: bool = False


class EntityConfig(BaseModel):
    """Present on entity (Kafka-publishing leaf) streams."""
    model_config = ConfigDict(extra="allow")

    kafka: KafkaOutput = Field(default_factory=KafkaOutput)
    schema_artifact_id: Optional[str] = None
    schema_version: Optional[int] = None
    iceberg: Optional[EntityIcebergConfig] = None


class SourceCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "name": "DummyJson Users",
                    "type": "REST API",
                    "base_url": "https://dummyjson.com",
                    "streams": [
                        {
                            "id": "users",
                            "name": "users",
                            "endpoint_path": "/users",
                            "method": "GET",
                            "response_format": "json",
                            "response_data_path": "$.users",
                            "is_primary": True,
                        }
                    ],
                },
                {
                    "name": "Assets Mongo",
                    "type": "MongoDB",
                    "mongo_connection_mode": "basic",
                    "db_host": "127.0.0.1",
                    "db_port": 27017,
                    "db_database": "assets_db",
                    "streams": [
                        {
                            "id": "assets",
                            "name": "assets",
                            "endpoint_path": "assets",
                            "method": "GET",
                            "response_format": "json",
                            "mongo_query": "{}",
                            "mongo_limit": 10,
                            "is_primary": True,
                        }
                    ],
                },
                {
                    "name": "SMB Asset Files",
                    "type": "SMB",
                    "smb_hostname": "172.16.20.9",
                    "smb_share": "cmdb",
                    "smb_username": "CMDB",
                    "smb_password": "change-me",
                    "smb_base_directory": "/",
                    "smb_file_filter": "^.+\\.csv$",
                    "smb_file_format": "CSV",
                    "smb_csv_delimiter": ",",
                    "smb_has_header_row": True,
                    "streams": [
                        {
                            "id": "assets",
                            "name": "assets",
                            "endpoint_path": "/",
                            "method": "GET",
                            "response_format": "json",
                            "is_primary": True,
                        }
                    ],
                },
                {
                    "name": "Webhook Orders",
                    "type": "Webhook",
                    "webhook_endpoint_id": "orders-created",
                    "webhook_secret": "change-me",
                    "webhook_signature_header": "X-Signature",
                    "webhook_signature_algo": "hmac-sha256",
                    "webhook_enabled": True,
                    "webhook_content_type": "json",
                    "streams": [
                        {
                            "id": "orders",
                            "name": "orders",
                            "method": "POST",
                            "response_format": "json",
                            "split_response_records": True,
                            "response_data_path": "$.orders",
                            "is_primary": True,
                        }
                    ],
                },
            ]
        },
    )

    name: str
    type: str  # REST API | PostgreSQL | MongoDB | SMB | Webhook | Trino
    connection_setup_mode: str = "manual"  # manual | application_service
    application_service_id: Optional[str] = None
    # REST API fields
    base_url: Optional[str] = None
    auth_type: Optional[str] = None  # None | API_KEY | BEARER | BASIC
    api_key_name: Optional[str] = None
    api_key_value: Optional[str] = None
    api_key_location: Optional[str] = None  # HEADER | QUERY
    bearer_token: Optional[str] = None
    rest_username: Optional[str] = None
    rest_password: Optional[str] = None
    connection_timeout: Optional[int] = 30
    read_timeout: Optional[int] = 30
    write_timeout: Optional[int] = 30
    idle_timeout: Optional[int] = 30
    rate_limit: Optional[int] = 120
    openapi_spec_id: Optional[str] = None
    openapi_selected_server_url: Optional[str] = None
    # PostgreSQL / MongoDB fields
    db_host: Optional[str] = None
    db_port: Optional[int] = None
    db_database: Optional[str] = None
    db_username: Optional[str] = None
    db_password: Optional[str] = None
    db_ssl_mode: Optional[str] = None
    db_connection_timeout: Optional[int] = None
    db_connection_uri: Optional[str] = None
    mongo_connection_mode: Optional[str] = None  # basic | uri | nifi_global_service
    nifi_service_refs: Dict[str, str] = Field(default_factory=dict)
    db_query: Optional[str] = None
    db_projection: Optional[str] = None
    db_sort: Optional[str] = None
    db_limit: Optional[int] = None
    # SMB fields
    smb_hostname: Optional[str] = None
    smb_username: Optional[str] = None
    smb_domain: Optional[str] = None
    smb_password: Optional[str] = None
    smb_share: Optional[str] = None
    smb_base_directory: Optional[str] = None
    smb_file_filter: Optional[str] = None
    smb_file_format: Optional[str] = None
    smb_csv_delimiter: Optional[str] = None
    smb_has_header_row: Optional[bool] = None
    smb_inject_org_from_filename: Optional[bool] = None  # inject /organization_name from filename
    smb_org_filename_delimiter: Optional[str] = None  # e.g. "__Assets" — split filename on this
    # Webhook fields
    webhook_endpoint_id: Optional[str] = None
    webhook_secret: Optional[str] = None
    webhook_signature_header: Optional[str] = None
    webhook_signature_algo: Optional[str] = None  # hmac-sha256
    webhook_enabled: Optional[bool] = True
    webhook_content_type: Optional[str] = None  # json | xml | text | auto
    # Trino / Iceberg fields
    trino_endpoint: Optional[str] = None
    trino_user: Optional[str] = "admin"
    trino_catalog: Optional[str] = None
    trino_schema: Optional[str] = None
    trino_table: Optional[str] = None
    trino_checkpoint_table: Optional[str] = None
    trino_checkpoint_catalog: Optional[str] = None
    trino_checkpoint_schema: Optional[str] = None
    trino_checkpoint_table_name: Optional[str] = None
    trino_auto_create_checkpoint_table: Optional[bool] = True
    trino_column_mode: Optional[str] = "manual"
    trino_columns: List[str] = Field(default_factory=list)
    trino_bootstrap_mode: Optional[str] = "latest_snapshot"
    trino_output_mode: Optional[str] = "one_message_per_row"
    trino_include_snapshot_metadata: Optional[bool] = True
    # Flow config
    schedule: SourceSchedule = Field(default_factory=SourceSchedule)
    streams: List[Stream] = Field(default_factory=list)
    kafka_output: KafkaOutput = Field(default_factory=KafkaOutput)
    # Full wizard designer state (stored for edit reload)
    designer_payload: Optional[Dict[str, Any]] = None

    @field_validator("name")
    @classmethod
    def _non_empty_name(cls, value: str) -> str:
        text = (value or "").strip()
        if not text:
            raise ValueError("source name must not be empty")
        return text

    @field_validator("type")
    @classmethod
    def _normalize_type(cls, value: str) -> str:
        text = (value or "").strip().lower().replace("_", " ")
        aliases = {
            "rest api": "REST API",
            "rest": "REST API",
            "mongodb": "MongoDB",
            "mongo": "MongoDB",
            "smb": "SMB",
            "webhook": "Webhook",
            "postgresql": "PostgreSQL",
            "postgres": "PostgreSQL",
            "trino": "Trino",
        }
        if text not in aliases:
            raise ValueError("type must be one of: REST API, MongoDB, SMB, Webhook, PostgreSQL, Trino")
        return aliases[text]

    @field_validator("connection_setup_mode")
    @classmethod
    def _validate_connection_setup_mode(cls, value: str) -> str:
        text = (value or "manual").strip().lower()
        if text not in {"manual", "application_service"}:
            raise ValueError("connection_setup_mode must be one of: manual, application_service")
        return text

    @field_validator("auth_type")
    @classmethod
    def _validate_auth_type(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return value
        text = value.strip().upper()
        if text not in {"NONE", "API_KEY", "BEARER", "BASIC"}:
            raise ValueError("auth_type must be one of: NONE, API_KEY, BEARER, BASIC")
        return text

    @field_validator("api_key_location")
    @classmethod
    def _validate_api_key_location(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return value
        text = value.strip().upper()
        if text not in {"HEADER", "QUERY"}:
            raise ValueError("api_key_location must be one of: HEADER, QUERY")
        return text

    @field_validator(
        "connection_timeout",
        "read_timeout",
        "write_timeout",
        "idle_timeout",
        "rate_limit",
        "db_connection_timeout",
        "db_limit",
    )
    @classmethod
    def _positive_optional_int(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and value < 1:
            raise ValueError("must be greater than or equal to 1")
        return value

    @field_validator("smb_file_format")
    @classmethod
    def _validate_smb_format(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return value
        text = value.strip().upper()
        if text not in {"CSV", "JSON", "JSONL", "XML", "XLSX", "XLS", "EXCEL", "BINARY"}:
            raise ValueError("smb_file_format must be one of: CSV, JSON, JSONL, XML, XLSX, XLS, EXCEL, BINARY")
        return "XLSX" if text in {"XLS", "EXCEL"} else text

    @field_validator("webhook_endpoint_id")
    @classmethod
    def _validate_webhook_endpoint_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        text = value.strip().lower()
        if not text:
            return None
        if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{2,127}", text):
            raise ValueError("webhook_endpoint_id must match [a-z0-9][a-z0-9._-]{2,127}")
        return text

    @field_validator("webhook_signature_algo")
    @classmethod
    def _validate_webhook_algo(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return "hmac-sha256"
        text = value.strip().lower()
        if text not in {"hmac-sha256"}:
            raise ValueError("webhook_signature_algo must be hmac-sha256")
        return text

    @field_validator("webhook_content_type")
    @classmethod
    def _validate_webhook_content_type(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return "auto"
        text = value.strip().lower()
        if text not in {"json", "xml", "text", "auto"}:
            raise ValueError("webhook_content_type must be one of: json, xml, text, auto")
        return text

    @model_validator(mode="after")
    def _validate_by_source_type(self):
        uses_application_service = self.connection_setup_mode == "application_service"
        if uses_application_service and not (self.application_service_id or "").strip():
            raise ValueError("application_service_id is required when connection_setup_mode is application_service")
        if uses_application_service and self.type not in {"REST API", "SMB", "Webhook"}:
            raise ValueError(f"Application Services are not supported for {self.type} sources")
        if self.type == "REST API" and not uses_application_service and not (self.base_url or "").strip():
            raise ValueError("base_url is required for REST API sources")
        if self.type == "REST API":
            stream_ids = {stream.id for stream in self.streams or []}
            for stream in self.streams or []:
                if stream.split_response_records and stream.wait_for_all_records_before_downstream:
                    raise ValueError(f"{stream.name}: split response and wait for all records cannot both be enabled")
                fan = stream.fan_out
                if not fan or not fan.enabled or fan.completion_mode != "after_all":
                    continue
                if not (fan.parent_stream_id or "").strip():
                    raise ValueError(f"{stream.name}: after_all fan-out requires parent_stream_id")
                if fan.parent_stream_id not in stream_ids:
                    raise ValueError(f"{stream.name}: after_all fan-out parent_stream_id must reference another stream")
                placeholder_text = "\n".join(
                    [
                        stream.endpoint_path or "",
                        stream.body_template or "",
                        "\n".join(f"{k}:{v}" for k, v in (stream.headers or {}).items()),
                        "\n".join(f"{k}:{v}" for k, v in (stream.query_params or {}).items()),
                    ]
                )
                placeholders = sorted(set(re.findall(r"\$\{([^}]+)\}", placeholder_text)))
                if placeholders:
                    raise ValueError(
                        f"{stream.name}: after_all fan-out cannot use per-record placeholders: {', '.join(placeholders)}"
                    )
        if self.type == "MongoDB":
            if not (self.db_database or "").strip():
                raise ValueError("db_database is required for MongoDB sources")
            mode = (self.mongo_connection_mode or "basic").strip().lower()
            if mode not in {"basic", "uri", "nifi_global_service"}:
                raise ValueError("mongo_connection_mode must be one of: basic, uri, nifi_global_service")
            if mode == "uri" and not (self.db_connection_uri or "").strip():
                raise ValueError("db_connection_uri is required when mongo_connection_mode is uri")
            if mode == "basic" and not (self.db_host or "").strip():
                raise ValueError("db_host is required when mongo_connection_mode is basic")
            if mode == "nifi_global_service" and not (self.nifi_service_refs or {}).get("mongodb"):
                raise ValueError("nifi_service_refs.mongodb is required when mongo_connection_mode is nifi_global_service")
        if self.type == "SMB" and not uses_application_service:
            missing = [
                field
                for field in ("smb_hostname", "smb_share", "smb_username", "smb_password")
                if not (getattr(self, field) or "").strip()
            ]
            if missing:
                raise ValueError(f"SMB source is missing required fields: {', '.join(missing)}")
        if self.type == "Webhook":
            if len(self.streams or []) != 1:
                raise ValueError("Webhook source must contain exactly one stream")
            if not (self.webhook_endpoint_id or "").strip():
                raise ValueError("webhook_endpoint_id is required for Webhook sources")
            stream = (self.streams or [None])[0]
            if stream:
                # Webhook stream is intentionally single and independent (no parent-child fan-out).
                if stream.fan_out and (
                    stream.fan_out.enabled
                    or stream.fan_out.parent_stream_id
                    or stream.fan_out.parent_field
                    or stream.fan_out.inject_as
                    or stream.fan_out.inject_field_name
                ):
                    raise ValueError("Webhook stream does not support parent-child request mapping")
                if stream.pagination and stream.pagination.enabled:
                    raise ValueError("Webhook stream does not support pagination")
        if self.type == "Trino":
            required = [
                ("trino_endpoint", self.trino_endpoint),
                ("trino_catalog", self.trino_catalog),
                ("trino_schema", self.trino_schema),
                ("trino_table", self.trino_table),
            ]
            for field_name, value in required:
                if not (value or "").strip():
                    raise ValueError(f"{field_name} is required for Trino sources")
            has_legacy_checkpoint = bool((self.trino_checkpoint_table or "").strip())
            has_split_checkpoint = all(
                (value or "").strip()
                for value in (
                    self.trino_checkpoint_catalog,
                    self.trino_checkpoint_schema,
                    self.trino_checkpoint_table_name,
                )
            )
            if not has_legacy_checkpoint and not has_split_checkpoint:
                raise ValueError(
                    "trino_checkpoint_table or trino_checkpoint_catalog/trino_checkpoint_schema/trino_checkpoint_table_name "
                    "is required for Trino sources"
                )
            column_mode = (self.trino_column_mode or "manual").strip().lower()
            if column_mode not in {"manual", "all"}:
                raise ValueError("trino_column_mode must be one of: manual, all")
            self.trino_column_mode = column_mode
            if column_mode == "manual" and not self.trino_columns:
                raise ValueError("trino_columns must include at least one column for Trino sources")
            for column in self.trino_columns:
                if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(column or "")):
                    raise ValueError("trino_columns must contain simple column names only")
            if self.trino_bootstrap_mode not in {"latest_snapshot"}:
                raise ValueError("trino_bootstrap_mode must be latest_snapshot")
            if self.trino_output_mode not in {"one_message_per_row"}:
                raise ValueError("trino_output_mode must be one_message_per_row")
        return self


class Source(SourceCreate):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    state: str = "Draft"  # Draft | Saved | Deployed
    nifi_process_group_id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
