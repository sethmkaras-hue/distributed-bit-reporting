import argparse
import asyncio
import json
import logging
import os

import uvicorn

from bitmesh.agent.providers import HardwareProvider
from bitmesh.agent.service import Agent
from bitmesh.cpu.api import create_app
from bitmesh.cpu.controller import Controller
from bitmesh.cpu.storage import Archive
from bitmesh.simulation.runtime import ProcessSimulation, Simulation, load_inventory, make_inventory


def main():
    parser = argparse.ArgumentParser(description="BITMesh control plane")
    parser.add_argument("mode", choices=["simulate", "cpu", "agent"])
    parser.add_argument("--inventory")
    parser.add_argument("--isolated-subsystems", action="store_true")
    parser.add_argument("--som-id")
    parser.add_argument("--subsystems", type=int, default=3)
    parser.add_argument("--soms", type=int, default=4)
    parser.add_argument("--parameters", type=int, default=12)
    parser.add_argument("--grpc-port", type=int, default=51000)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    inventory = (
        load_inventory(args.inventory)
        if args.inventory
        else make_inventory(args.subsystems, args.soms, args.parameters, args.grpc_port)
    )
    tls = (
        {k: os.environ[f"BITMESH_TLS_{k.upper()}"] for k in ("ca", "cert", "key")}
        if os.getenv("BITMESH_TLS_CA")
        else None
    )
    if args.mode == "agent":
        if not args.inventory or not args.som_id or not tls:
            parser.error("agent requires --inventory, --som-id and BITMESH_TLS_CA/CERT/KEY")
        node = next(n for n in inventory.nodes if n.id == args.som_id)
        agent = Agent(node, inventory, HardwareProvider(), tls=tls)

        async def run():
            await agent.start()
            try:
                await asyncio.Event().wait()
            finally:
                await agent.close()

        asyncio.run(run())
        return
    simulation_type = ProcessSimulation if args.isolated_subsystems else Simulation
    simulation = simulation_type(inventory) if args.mode == "simulate" else None
    if simulation and args.host not in {"127.0.0.1", "localhost"}:
        parser.error("unauthenticated simulation must bind loopback")
    if not simulation and (not args.inventory or not tls):
        parser.error("CPU requires --inventory and BITMESH_TLS_CA/CERT/KEY")
    archive = Archive(
        os.getenv(
            "BITMESH_DATABASE_URL",
            "sqlite:///.local/archive.db" if simulation else "postgresql+psycopg://bitmesh@127.0.0.1/bitmesh",
        )
    )
    controller = Controller(inventory, archive, tls=tls)
    tokens = json.loads(os.getenv("BITMESH_IDENTITIES", "{}"))
    app = create_app(controller, simulation, tokens=tokens, dev=simulation is not None)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
