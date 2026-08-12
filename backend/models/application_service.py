from datetime import datetime
from typing import Any, Dict, Optional
import uuid

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


APPLICATION_SERVICE_TYPES = {"REST API", "SMB", "Webhook"}
REST_AUTH_TYPES = {"NONE", "API_KEY", "BEARER", "BASIC"}
API_KEY_LOCATIONS = {"HEADER", "QUERY"}
SECRET_CONFIG_FIELDS = {
    "api_key_value",
    "bearer_token",
    "rest_password",
    "smb_password",
    "webhook_secret",
}


class ApplicationServiceCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    service_type: str
    description: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _non_empty_name(cls, value: str) -> str:
        text = (value or "").strip()
        if not text:
            raise ValueError("name must not be empty")
        return text

    @field_validator("service_type")
    @classmethod
    def _normalize_type(cls, value: str) -> str:
        text = (value or "").strip().lower().replace("_", " ")
        aliases = {
            "rest": "REST API",
            "rest api": "REST API",
            "api": "REST API",
            "smb": "SMB",
            "webhook": "Webhook",
        }
        if text not in aliases:
            raise ValueError("service_type must be one of: REST API, SMB, Webhook")
        return aliases[text]

    @model_validator(mode="after")
    def _validate_config(self):
        self.config = normalize_application_service_config(self.service_type, self.config)
        return self


class ApplicationServiceUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Optional[str] = None
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None

    @field_validator("name")
    @classmethod
    def _non_empty_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        text = value.strip()
        if not text:
            raise ValueError("name must not be empty")
        return text


class ApplicationService(ApplicationServiceCreate):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    health: str = "Not Tested"
    last_tested: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


def normalize_application_service_config(service_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
    cfg = dict(config or {})
    if service_type == "REST API":
        base_url = str(cfg.get("base_url") or "").strip()
        if not base_url:
            raise ValueError("config.base_url is required for REST API application services")
        auth_type = str(cfg.get("auth_type") or "NONE").strip().upper()
        if auth_type not in REST_AUTH_TYPES:
            raise ValueError("config.auth_type must be one of: NONE, API_KEY, BEARER, BASIC")
        cfg["base_url"] = base_url
        cfg["auth_type"] = auth_type
        if auth_type == "API_KEY":
            location = str(cfg.get("api_key_location") or "HEADER").strip().upper()
            if location not in API_KEY_LOCATIONS:
                raise ValueError("config.api_key_location must be one of: HEADER, QUERY")
            cfg["api_key_location"] = location
            if not str(cfg.get("api_key_name") or "").strip():
                raise ValueError("config.api_key_name is required for API key auth")
            if not str(cfg.get("api_key_value") or "").strip():
                raise ValueError("config.api_key_value is required for API key auth")
        elif auth_type == "BEARER":
            if not str(cfg.get("bearer_token") or "").strip():
                raise ValueError("config.bearer_token is required for bearer auth")
        elif auth_type == "BASIC":
            if not str(cfg.get("rest_username") or "").strip():
                raise ValueError("config.rest_username is required for basic auth")
            if not str(cfg.get("rest_password") or "").strip():
                raise ValueError("config.rest_password is required for basic auth")
        for key, default in (
            ("connection_timeout", 30),
            ("read_timeout", 30),
            ("write_timeout", 30),
            ("idle_timeout", 30),
            ("rate_limit", 120),
        ):
            cfg[key] = _positive_int(cfg.get(key), default, f"config.{key}")
        return cfg

    if service_type == "SMB":
        for key in ("smb_hostname", "smb_share", "smb_username", "smb_password"):
            if not str(cfg.get(key) or "").strip():
                raise ValueError(f"config.{key} is required for SMB application services")
        cfg["smb_hostname"] = str(cfg.get("smb_hostname") or "").strip()
        cfg["smb_share"] = str(cfg.get("smb_share") or "").strip()
        cfg["smb_username"] = str(cfg.get("smb_username") or "").strip()
        cfg["smb_domain"] = str(cfg.get("smb_domain") or "").strip() or None
        cfg["connection_timeout"] = _positive_int(cfg.get("connection_timeout"), 30, "config.connection_timeout")
        return cfg

    if service_type == "Webhook":
        if not str(cfg.get("webhook_secret") or "").strip():
            raise ValueError("config.webhook_secret is required for Webhook application services")
        cfg["webhook_signature_header"] = str(cfg.get("webhook_signature_header") or "X-Signature").strip()
        algo = str(cfg.get("webhook_signature_algo") or "hmac-sha256").strip().lower()
        if algo != "hmac-sha256":
            raise ValueError("config.webhook_signature_algo must be hmac-sha256")
        cfg["webhook_signature_algo"] = algo
        return cfg

    raise ValueError("service_type must be one of: REST API, SMB, Webhook")


def _positive_int(value: Any, default: int, field_name: str) -> int:
    if value is None or value == "":
        return default
    try:
        parsed = int(value)
    except Exception as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if parsed < 1:
        raise ValueError(f"{field_name} must be greater than or equal to 1")
    return parsed
