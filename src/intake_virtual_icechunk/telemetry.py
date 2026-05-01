# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field, replace
from typing import Any
from urllib import request
from uuid import uuid4


def _new_id() -> str:
    return uuid4().hex


@dataclass(slots=True)
class TelemetryContext:
    """Minimal lineage context for catalog search/load telemetry."""

    store_id: str
    trace_id: str = field(default_factory=_new_id)
    search_id: str | None = None
    search_params: dict[str, Any] | None = None
    search_result_count: int | None = None
    selection: dict[str, Any] | None = None
    extras: dict[str, Any] = field(default_factory=dict)

    def with_updates(self, **updates: Any) -> TelemetryContext:
        """Return a shallow-updated copy of this context."""

        return replace(self, **updates)

    def dataset_attrs(self) -> dict[str, Any]:
        """Return serialisable attrs to attach to materialised xarray datasets."""

        attrs = {
            "intake_virtual_icechunk_store_id": self.store_id,
            "intake_virtual_icechunk_trace_id": self.trace_id,
        }
        if self.search_id is not None:
            attrs["intake_virtual_icechunk_search_id"] = self.search_id
        if self.selection is not None and "key" in self.selection:
            attrs["intake_virtual_icechunk_key"] = self.selection["key"]
        return attrs


TelemetryEmitter = Callable[[str, TelemetryContext, Mapping[str, Any] | None], None]


def emit_telemetry(
    emitter: TelemetryEmitter | None,
    event: str,
    context: TelemetryContext,
    payload: Mapping[str, Any] | None = None,
) -> None:
    """Emit a telemetry event if an emitter has been configured."""

    if emitter is None:
        return
    emitter(event, context, payload)


def create_demo_http_emitter(
    url: str,
    *,
    timeout: float = 5.0,
    headers: Mapping[str, str] | None = None,
) -> TelemetryEmitter:
    """Return a demo emitter that POSTs telemetry events to an HTTP endpoint."""

    request_headers = {"content-type": "application/json"}
    request_headers.update(headers or {})

    def _emit(
        event: str,
        context: TelemetryContext,
        payload: Mapping[str, Any] | None,
    ) -> None:
        body = json.dumps(
            {
                "event": event,
                "context": asdict(context),
                "payload": dict(payload) if payload is not None else None,
            }
        ).encode("utf-8")
        req = request.Request(url, data=body, headers=request_headers, method="POST")
        with request.urlopen(req, timeout=timeout):
            pass

    return _emit
