"""Transactional archive. Receipt is returned only after the transaction commits."""

import json
import time
from pathlib import Path

from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    delete,
    event,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from bitmesh.models import Event, Measurement

metadata = MetaData()
records = Table(
    "records",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("kind", String(32), nullable=False, index=True),
    Column("key", String(512), nullable=False),
    Column("som_id", String(128), index=True),
    Column("time", Float, nullable=False, index=True),
    Column("expires", Float, index=True),
    Column("payload", Text, nullable=False),
    UniqueConstraint("kind", "key"),
)


class Archive:
    def __init__(self, url="sqlite:///.local/archive.db"):
        url_text = str(url)
        if url_text.startswith("sqlite:///:"):
            pass
        elif url_text.startswith("sqlite:///"):
            Path(url_text.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_engine(
            url,
            pool_pre_ping=True,
            connect_args={"check_same_thread": False} if url_text.startswith("sqlite") else {},
        )
        if self.engine.dialect.name == "sqlite":

            @event.listens_for(self.engine, "connect")
            def sqlite_durable(connection, _):
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=FULL")

        metadata.create_all(self.engine)

    def insert(self, conn, kind, key, payload, som_id=None, expires=None):
        values = dict(
            kind=kind,
            key=key,
            som_id=som_id,
            time=time.time(),
            expires=expires,
            payload=json.dumps(payload, allow_nan=False),
        )
        dialect = self.engine.dialect.name
        insert = pg_insert if dialect == "postgresql" else sqlite_insert
        conn.execute(insert(records).values(**values).on_conflict_do_nothing(index_elements=["kind", "key"]))

    def ingest_events(self, page: dict) -> int:
        events = [Event.model_validate(e) for e in page["events"]]
        if any(e.som_id != page["som_id"] or e.boot_id != page["boot_id"] for e in events):
            raise ValueError("event page source identity mismatch")
        with self.engine.begin() as conn:
            for e in events:
                self.insert(
                    conn, "event", f"{e.som_id}:{e.boot_id}:{e.sequence}", e.model_dump(mode="json"), e.som_id
                )
            for start, end in page.get("gaps", []):
                self.insert(
                    conn,
                    "gap",
                    f"{page['som_id']}:{page['boot_id']}:{start}:{end}",
                    {
                        "som_id": page["som_id"],
                        "boot_id": page["boot_id"],
                        "start": start,
                        "end": end,
                        "description": "Unarchived source events overwritten; historical event loss",
                    },
                    page["som_id"],
                )
            if page.get("gap_ranges_dropped", 0):
                self.insert(
                    conn,
                    "gap",
                    f"{page['som_id']}:{page['boot_id']}:unquantified",
                    {
                        "som_id": page["som_id"],
                        "description": "Additional old gap ranges lost from RAM",
                        "ranges_dropped": page["gap_ranges_dropped"],
                    },
                    page["som_id"],
                )
        # Transaction context has successfully COMMITTED before the caller may acknowledge.
        return max((e.sequence for e in events), default=0)

    def measurements(self, batch: list[Measurement]):
        now = time.time()
        values = [
            dict(
                kind="measurement",
                key=f"{m.som_id}:{m.boot_id}:{m.sequence}:{m.parameter.id}",
                som_id=m.som_id,
                time=now,
                expires=now + m.parameter.retention_s,
                payload=m.model_dump_json(),
            )
            for m in batch
            if m.parameter.retention_s
        ]
        if values:
            insert = pg_insert if self.engine.dialect.name == "postgresql" else sqlite_insert
            with self.engine.begin() as conn:
                conn.execute(insert(records).on_conflict_do_nothing(index_elements=["kind", "key"]), values)

    def save(self, kind: str, key: str, payload: dict, som_id=None):
        with self.engine.begin() as conn:
            conn.execute(delete(records).where(records.c.kind == kind, records.c.key == key))
            self.insert(conn, kind, key, payload, som_id)

    def get(self, kind: str, key: str):
        with self.engine.connect() as conn:
            value = conn.execute(
                select(records.c.payload).where(records.c.kind == kind, records.c.key == key)
            ).scalar_one_or_none()
        return json.loads(value) if value is not None else None

    def latest_members(self):
        latest = (
            select(records.c.som_id, func.max(records.c.id).label("latest_id"))
            .where(records.c.kind == "connectivity")
            .group_by(records.c.som_id)
            .subquery()
        )
        with self.engine.connect() as conn:
            rows = conn.execute(select(records.c.payload).join(latest, records.c.id == latest.c.latest_id))
            return [json.loads(row[0]) for row in rows]

    def list(self, kind: str, som_id: str | None = None, limit=500):
        query = select(records.c.payload).where(records.c.kind == kind)
        if som_id:
            query = query.where(records.c.som_id == som_id)
        with self.engine.connect() as conn:
            return [
                json.loads(row[0]) for row in conn.execute(query.order_by(records.c.id.desc()).limit(limit))
            ]

    def prune(self):
        with self.engine.begin() as conn:
            conn.execute(
                delete(records).where(records.c.expires.is_not(None), records.c.expires < time.time())
            )

    def close(self):
        self.engine.dispose()
