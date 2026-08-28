import base64
import json
import os
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request


BASE = os.environ.get("RAPID7_BASE", "https://apisix.datapasc.com/rapid7_asyad/api/3").rstrip("/")
USER = os.environ.get("RAPID7_USER", "apiuser")
PASSWORD = os.environ.get("RAPID7_PASSWORD")
DELAY_SECONDS = float(os.environ.get("RAPID7_INSPECT_DELAY", "1.5"))


if not PASSWORD:
    raise SystemExit("RAPID7_PASSWORD env var is required")


auth = base64.b64encode(f"{USER}:{PASSWORD}".encode("utf-8")).decode("ascii")
calls = []


def summarize(value, depth=0):
    if isinstance(value, dict):
        keys = list(value.keys())
        return {"type": "object", "keys": keys[:30], "key_count": len(keys)}
    if isinstance(value, list):
        return {"type": "array", "count": len(value), "first": summarize(value[0], depth + 1) if value else None}
    return {"type": type(value).__name__}


def get(path):
    url = BASE + path
    started = time.time()
    cmd = [
        "curl.exe",
        "-k",
        "-sS",
        "--connect-timeout",
        "10",
        "--max-time",
        "30",
        "-H",
        f"Authorization: Basic {auth}",
        "-H",
        "Accept: application/json",
        "-w",
        "\nHTTP_STATUS:%{http_code}",
        url,
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=40)
    elapsed_ms = int((time.time() - started) * 1000)
    marker = "\nHTTP_STATUS:"
    if marker in proc.stdout:
        body, status_text = proc.stdout.rsplit(marker, 1)
        try:
            status = int(status_text.strip()[:3])
        except ValueError:
            status = 0
    else:
        body = proc.stdout
        status = 0
    try:
        data = json.loads(body) if body.strip() else None
    except json.JSONDecodeError:
        data = None
    if proc.returncode == 0 and 200 <= status <= 299:
        calls.append({"path": path, "status": status, "elapsed_ms": elapsed_ms, "summary": summarize(data)})
        time.sleep(DELAY_SECONDS)
        return data
    calls.append({"path": path, "status": status, "curl_exit": proc.returncode, "elapsed_ms": elapsed_ms, "error_prefix": body[:160], "stderr": proc.stderr[:160]})
    time.sleep(DELAY_SECONDS)
    return None


def resources(data):
    if isinstance(data, dict) and isinstance(data.get("resources"), list):
        return data["resources"]
    return []


out = {"base": BASE, "sample_chain": {}, "calls": calls}

sites_page = get("/sites?page=0&size=1")
site_refs = resources(sites_page)
out["sample_chain"]["sites_page"] = {
    "resource_count": len(site_refs),
    "page": sites_page.get("page") if isinstance(sites_page, dict) else None,
    "resource_keys": list(site_refs[0].keys()) if site_refs else [],
}

if site_refs:
    site_id = site_refs[0].get("id")
    out["sample_chain"]["site_id_present"] = site_id is not None
    site_detail = get(f"/sites/{site_id}") if site_id is not None else None
    out["sample_chain"]["site_detail_keys"] = list(site_detail.keys()) if isinstance(site_detail, dict) else []

    site_assets_page = get(f"/sites/{site_id}/assets?page=0&size=1") if site_id is not None else None
    asset_refs = resources(site_assets_page)
    out["sample_chain"]["site_assets_page"] = {
        "resource_count": len(asset_refs),
        "page": site_assets_page.get("page") if isinstance(site_assets_page, dict) else None,
        "resource_keys": list(asset_refs[0].keys()) if asset_refs else [],
    }

    if asset_refs:
        asset_id = asset_refs[0].get("id")
        out["sample_chain"]["asset_id_present"] = asset_id is not None
        asset_detail = get(f"/assets/{asset_id}") if asset_id is not None else None
        out["sample_chain"]["asset_detail_keys"] = list(asset_detail.keys()) if isinstance(asset_detail, dict) else []
        if isinstance(asset_detail, dict):
            out["sample_chain"]["asset_detail_nested_shapes"] = {
                key: summarize(asset_detail.get(key))
                for key in ["addresses", "hostNames", "ids", "os", "software", "services", "vulnerabilities", "users", "groups", "databases", "files"]
                if key in asset_detail
            }

        services_page = get(f"/assets/{asset_id}/services?page=0&size=1") if asset_id is not None else None
        service_refs = resources(services_page)
        out["sample_chain"]["asset_services_page"] = {
            "resource_count": len(service_refs),
            "resource_keys": list(service_refs[0].keys()) if service_refs else [],
        }
        if service_refs:
            protocol = service_refs[0].get("protocol")
            port = service_refs[0].get("port")
            if protocol is not None and port is not None:
                service_detail = get(f"/assets/{asset_id}/services/{protocol}/{port}")
                out["sample_chain"]["service_detail_keys"] = list(service_detail.keys()) if isinstance(service_detail, dict) else []

        vulns_page = get(f"/assets/{asset_id}/vulnerabilities?page=0&size=1") if asset_id is not None else None
        vuln_refs = resources(vulns_page)
        out["sample_chain"]["asset_vulnerabilities_page"] = {
            "resource_count": len(vuln_refs),
            "resource_keys": list(vuln_refs[0].keys()) if vuln_refs else [],
        }
        if vuln_refs:
            vuln_id = vuln_refs[0].get("id")
            if vuln_id is not None:
                asset_vuln_detail = get(f"/assets/{asset_id}/vulnerabilities/{vuln_id}")
                out["sample_chain"]["asset_vulnerability_detail_keys"] = list(asset_vuln_detail.keys()) if isinstance(asset_vuln_detail, dict) else []
                vuln_detail = get(f"/vulnerabilities/{vuln_id}")
                out["sample_chain"]["vulnerability_detail_keys"] = list(vuln_detail.keys()) if isinstance(vuln_detail, dict) else []


json.dump(out, sys.stdout, indent=2)
sys.stdout.write("\n")
