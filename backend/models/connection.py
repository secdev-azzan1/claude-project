from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing import Optional
from datetime import datetime
import uuid


class ConnectionCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "name": "Kafka Local",
                    "type": "kafka",
                    "description": "Kafka bootstrap connection",
                    "endpoint": "localhost:9092",
                    "auth_type": "NONE",
                    "security_protocol": "PLAINTEXT",
                },
                {
                    "name": "Apache NiFi",
                    "type": "nifi",
                    "description": "NiFi API connection",
                    "endpoint": "https://nifi.example.com",
                    "auth_type": "BASIC",
                    "username": "admin",
                    "password": "change-me",
                },
            ]
        },
    )

    name: str
    type: str  # nifi | kafka | apicurio | kafka_connect | iceberg
    description: Optional[str] = None
    endpoint: str  # URL for NiFi/Apicurio, bootstrap servers for Kafka
    # Generic auth
    auth_type: str = "NONE"  # NONE | BEARER | BASIC | CLIENT_CERT
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    # Kafka-specific
    kafka_connection_mode: Optional[str] = "native"
    security_protocol: Optional[str] = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    default_topic_prefix: Optional[str] = None
    kafbat_url: Optional[str] = None
    kafbat_username: Optional[str] = None
    kafbat_password: Optional[str] = None
    # Apicurio-specific
    group_id: Optional[str] = "nif-platform"
    # Iceberg-specific (catalog + object store)
    iceberg_warehouse: Optional[str] = "bronze"
    iceberg_credential: Optional[str] = None
    iceberg_oauth2_server_uri: Optional[str] = None
    iceberg_scope: Optional[str] = "PRINCIPAL_ROLE:ALL"
    s3_endpoint: Optional[str] = None
    s3_access_key_id: Optional[str] = None
    s3_secret_access_key: Optional[str] = None
    s3_region: Optional[str] = "us-east-1"
    s3_path_style_access: Optional[bool] = True

    @field_validator("name", "endpoint")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        text = (value or "").strip()
        if not text:
            raise ValueError("must not be empty")
        return text

    @field_validator("type")
    @classmethod
    def _normalize_type(cls, value: str) -> str:
        text = (value or "").strip().lower()
        aliases = {
            "nifi": "nifi",
            "apache nifi": "nifi",
            "kafka": "kafka",
            "apicurio": "apicurio",
            "schema registry": "apicurio",
            "kafka_connect": "kafka_connect",
            "kafka connect": "kafka_connect",
            "connect": "kafka_connect",
            "iceberg": "iceberg",
            "iceberg catalog": "iceberg",
            "polaris": "iceberg",
        }
        if text not in aliases:
            raise ValueError("type must be one of: nifi, kafka, apicurio, kafka_connect, iceberg")
        return aliases[text]

    @field_validator("auth_type")
    @classmethod
    def _normalize_auth_type(cls, value: str) -> str:
        text = (value or "NONE").strip().upper()
        if text not in {"NONE", "BEARER", "BASIC", "CLIENT_CERT"}:
            raise ValueError("auth_type must be one of: NONE, BEARER, BASIC, CLIENT_CERT")
        return text

    @field_validator("kafka_connection_mode")
    @classmethod
    def _normalize_kafka_connection_mode(cls, value: Optional[str]) -> str:
        text = (value or "native").strip().lower()
        if text not in {"native", "kafbat"}:
            raise ValueError("kafka_connection_mode must be one of: native, kafbat")
        return text


class ConnectionUpdate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "description": "Updated endpoint",
                    "endpoint": "localhost:9092",
                    "security_protocol": "PLAINTEXT",
                }
            ]
        },
    )

    name: Optional[str] = None
    description: Optional[str] = None
    endpoint: Optional[str] = None
    auth_type: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    security_protocol: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    default_topic_prefix: Optional[str] = None
    kafka_connection_mode: Optional[str] = None
    kafbat_url: Optional[str] = None
    kafbat_username: Optional[str] = None
    kafbat_password: Optional[str] = None
    group_id: Optional[str] = None
    # Iceberg-specific (catalog + object store) — must mirror ConnectionCreate, otherwise
    # editing an iceberg connection fails validation because this model is extra="forbid".
    iceberg_warehouse: Optional[str] = None
    iceberg_credential: Optional[str] = None
    iceberg_oauth2_server_uri: Optional[str] = None
    iceberg_scope: Optional[str] = None
    s3_endpoint: Optional[str] = None
    s3_access_key_id: Optional[str] = None
    s3_secret_access_key: Optional[str] = None
    s3_region: Optional[str] = None
    s3_path_style_access: Optional[bool] = None

    @field_validator("name", "endpoint")
    @classmethod
    def _non_empty_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        text = value.strip()
        if not text:
            raise ValueError("must not be empty")
        return text

    @field_validator("auth_type")
    @classmethod
    def _normalize_auth_type(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        text = value.strip().upper()
        if text not in {"NONE", "BEARER", "BASIC", "CLIENT_CERT"}:
            raise ValueError("auth_type must be one of: NONE, BEARER, BASIC, CLIENT_CERT")
        return text

    @field_validator("kafka_connection_mode")
    @classmethod
    def _normalize_kafka_connection_mode_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        text = value.strip().lower()
        if text not in {"native", "kafbat"}:
            raise ValueError("kafka_connection_mode must be one of: native, kafbat")
        return text


class ConnectionRepoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    endpoint: str
    auth_type: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    strategy: str  # adopt | migrate | reset
    pace: Optional[str] = None  # all | one_by_one
    abandon_old: Optional[bool] = None


class Connection(ConnectionCreate):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    health: str = "Not Tested"  # Healthy | Failed | Not Tested
    last_tested: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = False
    reachability: str = "Unknown"  # Reachable | Unreachable | Unknown
