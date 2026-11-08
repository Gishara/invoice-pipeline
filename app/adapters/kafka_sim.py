"""A Kafka-like broker with at-least-once delivery and consumer groups.

State lives in its own SQLite file: partition logs plus committed offsets (like
Kafka's __consumer_offsets). A consumer group splits partitions among its members;
one member owns a partition at a time, so adding consumers scales ingest up to the
partition count. A consumer that dies before committing gets its records again."""
from __future__ import annotations

import random
import sqlite3
import threading
import zlib

from app.ports import Record


class Broker:
    def __init__(self, path: str):
        self.path = path
        self._local = threading.local()
        self._write = threading.Lock()
        self._db().executescript("""
        CREATE TABLE IF NOT EXISTS log(topic TEXT, partition INT, offset INT, key TEXT, value BLOB,
                                       PRIMARY KEY(topic, partition, offset));
        CREATE TABLE IF NOT EXISTS committed(grp TEXT, topic TEXT, partition INT, next_offset INT,
                                             PRIMARY KEY(grp, topic, partition));
        CREATE TABLE IF NOT EXISTS topics(topic TEXT PRIMARY KEY, partitions INT);""")

    def _db(self) -> sqlite3.Connection:
        c = getattr(self._local, "c", None)
        if c is None:
            c = sqlite3.connect(self.path, isolation_level=None, timeout=60)
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA busy_timeout=60000")
            self._local.c = c
        return c

    def create_topic(self, topic: str, partitions: int):
        self._db().execute("INSERT OR IGNORE INTO topics VALUES(?,?)", (topic, partitions))

    def partitions(self, topic: str) -> int:
        return self._db().execute("SELECT partitions FROM topics WHERE topic=?", (topic,)).fetchone()[0]

    def end_offset(self, topic: str, p: int) -> int:
        r = self._db().execute("SELECT MAX(offset) FROM log WHERE topic=? AND partition=?", (topic, p)).fetchone()[0]
        return 0 if r is None else r + 1

    def append(self, topic: str, key: str, value: bytes) -> tuple[int, int]:
        p = zlib.crc32(key.encode()) % self.partitions(topic)  # same key -> same partition -> ordered
        with self._write:
            off = self.end_offset(topic, p)
            self._db().execute("INSERT INTO log VALUES(?,?,?,?,?)", (topic, p, off, key, value))
        return p, off

    def read(self, topic: str, p: int, from_offset: int, limit: int):
        return self._db().execute("SELECT offset, key, value FROM log WHERE topic=? AND partition=? AND offset>=?"
                                  " ORDER BY offset LIMIT ?", (topic, p, from_offset, limit)).fetchall()

    def committed(self, grp: str, topic: str, p: int) -> int:
        r = self._db().execute("SELECT next_offset FROM committed WHERE grp=? AND topic=? AND partition=?",
                               (grp, topic, p)).fetchone()
        return r[0] if r else 0

    def commit(self, grp: str, topic: str, p: int, next_offset: int):
        self._db().execute("INSERT INTO committed VALUES(?,?,?,?) ON CONFLICT(grp,topic,partition)"
                           " DO UPDATE SET next_offset=MAX(next_offset, excluded.next_offset)", (grp, topic, p, next_offset))

    def reset_group(self, grp: str):
        self._db().execute("DELETE FROM committed WHERE grp=?", (grp,))

    def lag(self, grp: str, topic: str) -> int:
        return sum(self.end_offset(topic, p) - self.committed(grp, topic, p) for p in range(self.partitions(topic)))


class Producer:
    def __init__(self, broker: Broker, dup_prob: float = 0.0, seed: int = 1):
        self.b, self.dup, self.rnd = broker, dup_prob, random.Random(seed)
        self.sent = self.duplicated = 0

    def send(self, topic: str, key: str, value: bytes):
        self.b.append(topic, key, value)
        self.sent += 1
        if self.rnd.random() < self.dup:  # the ack was lost, the producer retried, both landed
            self.b.append(topic, key, value)
            self.duplicated += 1


class GroupConsumer:
    """One member of a consumer group with a static share of the partitions
    (member i of n owns partitions p where p % n == i)."""

    def __init__(self, broker: Broker, topic: str, group: str, member: int, members: int):
        self.b, self.topic, self.group = broker, topic, group
        self.owned = [p for p in range(broker.partitions(topic)) if p % members == member]
        self.restart()

    def restart(self):
        self.pos = {p: self.b.committed(self.group, self.topic, p) for p in self.owned}

    def poll(self, max_records: int = 20) -> list[Record]:
        out = []
        for p in self.owned:
            for off, key, val in self.b.read(self.topic, p, self.pos[p], max_records):
                out.append(Record(self.topic, p, off, key, val))
                self.pos[p] = off + 1
        return out

    def commit(self, rec: Record):
        self.b.commit(self.group, rec.topic, rec.partition, rec.offset + 1)
