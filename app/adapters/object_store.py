"""Content-addressed object store on the local file system (stands in for S3/GCS)."""
from __future__ import annotations

import os
from pathlib import Path

from app.domain.model import sha256


class FileObjectStore:
    def __init__(self, root: Path):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)

    def put(self, data: bytes) -> str:
        h = sha256(data)
        p = self.root / h[:2] / f"{h}.pdf"
        if not p.exists():  # immutable: a second write of the same bytes is a no-op
            p.parent.mkdir(exist_ok=True)
            tmp = p.with_suffix(f".{os.getpid()}.{id(data)}.tmp")
            tmp.write_bytes(data)
            tmp.replace(p)
        return str(p.relative_to(self.root))

    def get(self, ref: str) -> bytes:
        return (self.root / ref).read_bytes()
