"""Stable placement of a vector in a logical shard (decision D01).

`shard = blake2b(collection, id) mod S`. Python's built-in `hash()` is randomized per process
(PYTHONHASHSEED), so it must never be used for placement.
"""

from __future__ import annotations

import hashlib


def shard_for(collection: str, vector_id: str, num_shards: int) -> int:
    """Return the logical shard owning (`collection`, `vector_id`). Depends only on its inputs."""
    if num_shards < 1:
        raise ValueError("num_shards must be >= 1")
    # Length prefix: ("ab", "c") and ("a", "bc") must not collide.
    key = f"{len(collection)}:{collection}/{vector_id}".encode()
    digest = hashlib.blake2b(key, digest_size=8).digest()
    return int.from_bytes(digest, "big") % num_shards
