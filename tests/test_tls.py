import subprocess

import grpc
import pytest

from bitmesh.agent.providers import MockProvider
from bitmesh.agent.service import Agent
from bitmesh.models import Command
from bitmesh.simulation.runtime import make_inventory
from bitmesh.wire import Transport
from conftest import free_port_block


def certificate(tmp_path, name):
    def openssl(*args):
        subprocess.run(
            ["openssl", *args], cwd=tmp_path, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )

    if not (tmp_path / "ca.pem").exists():
        openssl(
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=test-ca",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
        )
    openssl(
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-subj",
        f"/CN={name}",
        "-keyout",
        f"{name}.key",
        "-out",
        f"{name}.csr",
    )
    (tmp_path / "extensions").write_text(
        "subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n"
    )
    openssl(
        "x509",
        "-req",
        "-in",
        f"{name}.csr",
        "-CA",
        "ca.pem",
        "-CAkey",
        "ca.key",
        "-CAcreateserial",
        "-days",
        "1",
        "-extfile",
        "extensions",
        "-out",
        f"{name}.pem",
    )
    return {
        "ca": str(tmp_path / "ca.pem"),
        "cert": str(tmp_path / f"{name}.pem"),
        "key": str(tmp_path / f"{name}.key"),
    }


async def test_mtls_identity_binding_and_bootstrap_recovery(tmp_path):
    inventory = make_inventory(subsystems=1, port=free_port_block())
    node = inventory.nodes[0]
    agent = Agent(node, inventory, MockProvider(), tls=certificate(tmp_path, node.id))
    cpu = Transport(certificate(tmp_path, "cpu"))
    stranger = Transport(certificate(tmp_path, "stranger"))
    await agent.start()
    try:
        result = await cpu.call(node, "Identity", {}, timeout=2)
        assert result["lifecycle"] == "QUARANTINED"
        with pytest.raises(grpc.aio.AioRpcError) as rejected:
            await stranger.call(node, "Identity", {}, timeout=2)
        assert rejected.value.code() == grpc.StatusCode.PERMISSION_DENIED
        with pytest.raises(grpc.aio.AioRpcError):
            await cpu.call(node, "Peer", {"action": "heartbeat", "identity": agent.identity()}, timeout=2)
        command = Command(target=node.id, boot_id=agent.boot_id, kind="approve", operator="admin")
        result = await cpu.call(
            node, "Command", {"authority": {}, "command": command.model_dump(mode="json")}, timeout=2
        )
        assert result["state"] == "SUCCEEDED"
        command = Command(target=node.id, boot_id=agent.boot_id, kind="start", operator="admin")
        with pytest.raises(grpc.aio.AioRpcError):
            await cpu.call(
                node, "Command", {"authority": {}, "command": command.model_dump(mode="json")}, timeout=2
            )
    finally:
        await cpu.close()
        await stranger.close()
        await agent.close()
