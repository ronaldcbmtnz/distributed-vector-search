"""Pure shard logic: validation, filters, exact index, dedup, replication log, merge.

Owner: A. Must not import ray or any network library (reused unchanged in phase 2).
"""
