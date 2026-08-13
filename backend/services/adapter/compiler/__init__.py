"""The flow COMPILER (T7.1): a pure function `Flow -> DeploymentPlan`.

    from services.adapter.compiler import compile_flow, CompileContext, CompileError
    plan = compile_flow(flow, ctx)
    plan.to_dict() / plan.to_json()

See `docs/orchestration/compiler-spec.md` for the specification and
`compile_flow.py`'s module docstring for the compile orchestration. No
network I/O happens anywhere in this package — applying a `DeploymentPlan` to
a live NiFi/Kafka Connect instance is a separate, later task.
"""

from .compile_flow import compile_flow
from .ir import CompileContext, CompileError, DeploymentPlan

__all__ = ["compile_flow", "CompileContext", "CompileError", "DeploymentPlan"]
