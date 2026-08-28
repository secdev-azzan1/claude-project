# FortiSIEM Connectivity Check

Date: 2026-08-16

## Verdict

The remote NiFi runtime reached FortiSIEM at:

`GET /phoenix/rest/config/Domain` on `https://172.16.30.6:443`

Authentication form used:

`basic` with username `super/CMDBAPI`  
The domain prefix was embedded in the username as requested.

Observed outcome from a temporary NiFi flow:

- The flow was deployed on the live remote NiFi instance, not the local host.
- The child NiFi process group executed.
- `GenerateFlowFile` ran once.
- `UpdateAttribute` ran once.
- `InvokeHTTP` ran once and produced a response FlowFile.
- No NiFi bulletins were observed during the successful run.
- No TLS trust, hostname/SNI, timeout, or auth-failure bulletin appeared.

Processor counts from the final successful run:

- `trigger`: `files_out=1`
- `init`: `files_in=1`, `files_out=1`
- `fetch`: `files_in=1`, `files_out=1`
- `convert`: `files_in=0`, `files_out=0`
- `split`: `files_in=0`, `files_out=0`

Interpretation:

- Network reachability: pass
- TLS handshake: pass
- HTTP request: response observed from the remote NiFi `InvokeHTTP` processor
- Authentication: no auth failure surfaced

I did not capture the numeric HTTP status code before cleanup, so I am not claiming a specific 2xx/4xx value in this note.

## Cleanup

- The temporary NiFi process group created for this probe was deleted successfully.
- The temporary NiFi parameter context created for this probe was deleted successfully.
- No password or other secret values were written to this note.
