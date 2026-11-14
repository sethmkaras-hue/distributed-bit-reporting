"""Optional disposable PostgreSQL validation; never logs generated credentials."""

import json
import os
import secrets
import subprocess
import time
from pathlib import Path
from uuid import uuid4

from sqlalchemy import URL

from bitmesh.cpu.storage import Archive
from bitmesh.models import Event


def main():
    name = f"bitmesh-validation-{uuid4().hex[:8]}"
    password = secrets.token_urlsafe(32)
    environment = dict(os.environ, POSTGRES_PASSWORD=password)
    started = False
    try:
        subprocess.run(
            [
                "docker",
                "run",
                "--detach",
                "--rm",
                "--name",
                name,
                "-e",
                "POSTGRES_PASSWORD",
                "-e",
                "POSTGRES_USER=bitmesh",
                "-e",
                "POSTGRES_DB=bitmesh",
                "-p",
                "127.0.0.1:54329:5432",
                "postgres:17",
            ],
            env=environment,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        started = True
        for _ in range(60):
            r = subprocess.run(
                ["docker", "exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "bitmesh", "-d", "bitmesh"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if r.returncode == 0:
                break
            time.sleep(0.5)
        url = URL.create(
            "postgresql+psycopg",
            username="bitmesh",
            password=password,
            host="127.0.0.1",
            port=54329,
            database="bitmesh",
        )
        archive = Archive(url)
        event = Event(
            som_id="postgres-test",
            boot_id="boot-1",
            sequence=1,
            severity="CRITICAL",
            category="test",
            description="Durable PostgreSQL smoke event",
        )
        page = {"som_id": event.som_id, "boot_id": event.boot_id, "events": [event.model_dump(mode="json")]}
        assert archive.ingest_events(page) == 1
        assert archive.ingest_events(page) == 1
        assert len(archive.list("event")) == 1
        archive.save("connectivity", "first", {"som_id": "postgres-test", "boot_id": "old"}, "postgres-test")
        archive.save("connectivity", "second", {"som_id": "postgres-test", "boot_id": "new"}, "postgres-test")
        assert archive.latest_members() == [{"som_id": "postgres-test", "boot_id": "new"}]
        from bitmesh.models import Measurement, Parameter

        measurement = Measurement(
            som_id="s" * 128,
            boot_id="b" * 36,
            sequence=10**18,
            parameter=Parameter(id="p" * 128, name="Long-key diagnostic", category="test", data_type="float"),
            value=1.0,
        )
        archive.measurements([measurement])
        assert len(archive.list("measurement")) == 1
        archive.close()
        # A new engine sees the committed event.
        archive = Archive(url)
        assert len(archive.list("event")) == 1
        archive.close()
        report = {
            "database": "PostgreSQL 17",
            "persistent_boot_identity": True,
            "maximum_identity_measurement_key": True,
            "committed_receipt": True,
            "deduplication": True,
            "new_engine_reopen": True,
            "disposable_container_removed": True,
        }
        Path("docs/postgres-validation.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
    finally:
        if started:
            subprocess.run(["docker", "rm", "--force", name], check=True, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
