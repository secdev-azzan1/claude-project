"""T7.2+T7.3+T7.4 — apply DeploymentPlans to live NiFi + Kafka Connect, and
the flow lifecycle verbs that drive them.

Modules:
  - `nifi_apply`    — DeploymentPlan.rootGroup -> live NiFi process groups,
                       controller services, processors, ports, connections.
  - `connect_apply` — DeploymentPlan.connectors -> live Kafka Connect
                       connectors.
  - `topics`        — DeploymentPlan.topics -> live Kafka topics (Kafbat
                       path — see services/kafka_client.py's docstring on why
                       the broker isn't reachable by TCP from this host).
  - `lifecycle`      — the flow verb implementations (deploy/start/pause/
                       resume/stop/stop_clear/undeploy/delete/
                       clear_dedup_cache) operating on a flow doc + db,
                       wired up by routers/v2/flows.py.

Nothing in `nifi_apply` / `connect_apply` / `topics` touches a database —
they take plain connection dicts (the same `{endpoint, auth_type, username,
password, token}` / `{endpoint, security_protocol, ...}` shapes
`services/nifi_client.py` and `services/kafka_client.py` already use) and a
compiled `DeploymentPlan` (or a slice of one). `lifecycle` is the only module
here that reads/writes Mongo — it is what translates `connections_v2`
documents into those plain connection dicts before calling into the other
three.
"""

from __future__ import annotations
