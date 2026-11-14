"""Browser boundary. Production requires configured identities and HTTPS termination."""

import asyncio
import hmac
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import Field

from bitmesh.models import Command, Model


class OperatorIdentity(Model):
    name: str = Field(min_length=1, max_length=128)
    role: Literal["viewer", "operator", "admin"]


class SocketAuth(Model):
    token: str = Field(default="", max_length=4096)


class CommandRequest(Model):
    id: str = Field(default_factory=lambda: str(uuid4()))
    target: str
    boot_id: str
    kind: str
    arguments: dict = Field(default_factory=dict)
    timeout_s: float = Field(default=15, ge=0.1, le=300)


class Injection(Model):
    target: str
    action: Literal[
        "temperature",
        "cpu",
        "memory",
        "interface",
        "disconnect",
        "reconnect",
        "master_failure",
        "quarantine",
        "reboot",
        "overflow",
        "ptp",
    ]
    value: str | float | None = None


def create_app(controller, simulation=None, tokens=None, dev=False):
    tokens = {
        key: OperatorIdentity.model_validate(value).model_dump() for key, value in (tokens or {}).items()
    }
    if any(not key for key in tokens):
        raise ValueError("empty bearer credentials are forbidden")
    if not dev and not tokens:
        raise ValueError("production requires configured bearer identities")

    @asynccontextmanager
    async def lifespan(app):
        try:
            if simulation:
                await simulation.start()
            await controller.start()
            yield
        finally:
            await controller.close()
            if simulation:
                await simulation.close()
            controller.archive.close()

    app = FastAPI(title="BITMesh CPU", version="0.1.0", lifespan=lifespan)

    def identify(token):
        for secret, identity in tokens.items():
            if hmac.compare_digest(secret, token):
                return identity
        if dev and not token:
            return {"name": "local-simulator", "role": "admin"}
        raise HTTPException(401, "Authentication required")

    def user(authorization: str = Header(default="")):
        return identify(
            authorization.partition(" ")[2] if authorization.startswith("Bearer") else authorization
        )

    @app.get("/api/health")
    async def health():
        return {
            "status": "ok",
            "mode": "simulation" if simulation else "hardware",
            "archive_error": controller.last_error,
        }

    @app.get("/api/state")
    async def state(identity=Depends(user)):
        return {**controller.state(), "operator": identity, "simulation": simulation is not None}

    @app.get("/api/archive/{kind}")
    async def archive(kind: str, som_id: str | None = None, limit: int = 500, identity=Depends(user)):
        if kind not in {
            "event",
            "gap",
            "command",
            "configuration",
            "audit",
            "approval",
            "measurement",
            "leadership",
            "connectivity",
            "ack",
            "inventory",
        }:
            raise HTTPException(404, "Unknown archive")
        return await asyncio.to_thread(controller.archive.list, kind, som_id, max(1, min(limit, 2000)))

    @app.post("/api/events/{som_id}/{boot_id}/{sequence}/ack")
    async def acknowledge(som_id: str, boot_id: str, sequence: int, identity=Depends(user)):
        if identity["role"] not in {"operator", "admin"}:
            raise HTTPException(403, "Operator required")
        key = f"{som_id}:{boot_id}:{sequence}"
        if not await asyncio.to_thread(controller.archive.get, "event", key):
            raise HTTPException(404, "Archived event not found")
        payload = {
            "som_id": som_id,
            "boot_id": boot_id,
            "sequence": sequence,
            "operator": identity["name"],
            "time": time.time(),
        }
        await asyncio.to_thread(controller.archive.save, "ack", key, payload, som_id)
        return payload

    @app.post("/api/commands")
    async def command(request: CommandRequest, identity=Depends(user)):
        try:
            command = Command(**request.model_dump(), operator=identity["name"])
            return await controller.dispatch(command, identity["role"])
        except PermissionError as exc:
            await asyncio.to_thread(
                controller.archive.save,
                "audit",
                str(uuid4()),
                {
                    "operator": identity["name"],
                    "rejected_command": request.model_dump(),
                    "error": str(exc),
                    "time": time.time(),
                },
                request.target,
            )
            raise HTTPException(403, str(exc)) from exc
        except (ValueError, KeyError) as exc:
            await asyncio.to_thread(
                controller.archive.save,
                "audit",
                str(uuid4()),
                {
                    "operator": identity["name"],
                    "rejected_command": request.model_dump(),
                    "error": str(exc),
                    "time": time.time(),
                },
                request.target,
            )
            raise HTTPException(422, str(exc)) from exc

    @app.post("/api/simulation/inject")
    async def inject(request: Injection, identity=Depends(user)):
        if not simulation or identity["role"] != "admin":
            raise HTTPException(403, "Simulation admin required")
        try:
            if request.action == "overflow" and (
                float(request.value or 100) > 10000 or float(request.value or 100) < 1
            ):
                raise ValueError("overflow count must be 1–10000")
            result = await simulation.inject(request.target, request.action, request.value)
            await asyncio.to_thread(
                controller.archive.save,
                "audit",
                str(uuid4()),
                {"operator": identity["name"], "injection": request.model_dump(), "time": time.time()},
                request.target,
            )
            return result
        except (ValueError, TypeError) as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.websocket("/api/live")
    async def websocket(ws: WebSocket):
        await ws.accept()
        queue = asyncio.Queue(maxsize=16)
        try:
            # Token travels in the first frame, never URL/query logs.
            auth = SocketAuth.model_validate(await asyncio.wait_for(ws.receive_json(), 5))
            identify(auth.token)
            if len(controller.subscribers) >= 8:
                await ws.close(code=1013, reason="Live client capacity reached")
                return
            controller.subscribers.add(queue)
            await ws.send_json({"type": "state", "state": controller.state()})
            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), 1)
                except TimeoutError:
                    message = {"type": "heartbeat", "cpu_time": time.time()}
                await asyncio.wait_for(ws.send_json(message), 2)
        except (WebSocketDisconnect, TimeoutError, HTTPException, RuntimeError, ValueError):
            pass
        finally:
            controller.subscribers.discard(queue)
            try:
                await ws.close()
            except RuntimeError:
                pass

    dist = Path(__file__).resolve().parents[2] / "web" / "dist"
    if dist.exists():
        app.mount("/assets", StaticFiles(directory=dist / "assets"), name="assets")

        @app.get("/{path:path}")
        async def frontend(path: str):
            return FileResponse(dist / "index.html")

    return app
