# Rapid7 Connectivity Check

Date: 2026-08-16

## Verdict

The remote NiFi runtime did not get to an HTTP response from `https://172.16.20.55:3780/api/3/administration/info`.

Observed outcome from a temporary NiFi flow:

- `GenerateFlowFile -> InvokeHTTP` was created inside the live NiFi instance.
- `InvokeHTTP` emitted repeated bulletins with:
  - `javax.net.ssl.SSLHandshakeException`
  - `PKIX path building failed`
  - `unable to find valid certification path to requested target`
- No HTTP status code was observed.
- Authentication was not reached, because the request failed during TLS verification.

This classifies as a **TLS trust / certificate-chain failure**, not an HTTP 401/403 auth failure and not an endpoint-path failure.

## Evidence

- Temporary NiFi process group created for the probe: `rapid7-connectivity-20260816-084946`
- InvokeHTTP target: `https://172.16.20.55:3780/api/3/administration/info`
- NiFi bulletin samples recorded during the probe:
  - `SSLHandshakeException`
  - `PKIX path building failed`
  - `unable to find valid certification path to requested target`

## Cleanup

- Temporary NiFi process group `rapid7-connectivity-20260816-084946` was deleted successfully.
- A second aborted retry created `rapid7-connectivity-20260816-085051`; it was also deleted successfully.
- No secret values were written to this note or to the probe output.
