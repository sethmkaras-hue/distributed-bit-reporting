import asyncio
import json
from pathlib import Path

import grpc

from bitmesh import mesh_pb2 as pb
from bitmesh import mesh_pb2_grpc as rpc

MAX_WIRE = 4 * 1024 * 1024


def pack(value: dict) -> pb.Envelope:
    data = json.dumps(value, allow_nan=False, separators=(",", ":")).encode()
    if len(data) > MAX_WIRE:
        raise ValueError("message exceeds wire budget")
    return pb.Envelope(schema_version=1, json=data)


def unpack(envelope: pb.Envelope) -> dict:
    if envelope.schema_version != 1 or len(envelope.json) > MAX_WIRE:
        raise ValueError("unsupported schema or oversized message")
    value = json.loads(envelope.json)
    if not isinstance(value, dict):
        raise ValueError("object payload required")
    return value


class Transport:
    def __init__(self, tls: dict | None = None, timeout_s=0.25):
        self.tls = tls
        self.timeout_s = timeout_s
        self.channels = {}
        self.stubs = {}

    def stub(self, endpoint: str):
        if endpoint not in self.channels:
            options = [
                ("grpc.max_receive_message_length", MAX_WIRE + 1024),
                ("grpc.max_send_message_length", MAX_WIRE + 1024),
            ]
            if self.tls:
                creds = grpc.ssl_channel_credentials(
                    *(Path(self.tls[k]).read_bytes() for k in ("ca", "key", "cert"))
                )
                channel = grpc.aio.secure_channel(endpoint, creds, options=options)
            else:
                if endpoint.split(":")[0] not in {"127.0.0.1", "localhost"}:
                    raise ValueError("plaintext gRPC is restricted to loopback")
                channel = grpc.aio.insecure_channel(endpoint, options=options)
            self.channels[endpoint] = channel
            self.stubs[endpoint] = rpc.AgentStub(channel)
        return self.stubs[endpoint]

    async def call(self, node, method: str, value: dict, timeout: float | None = None) -> dict:
        result = await getattr(self.stub(node.endpoint), method)(
            pack(value), timeout=self.timeout_s if timeout is None else timeout
        )
        return unpack(result)

    async def close(self):
        await asyncio.gather(*(c.close() for c in self.channels.values()))
        self.channels.clear()
        self.stubs.clear()
