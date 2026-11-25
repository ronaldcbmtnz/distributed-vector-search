"""Transport-agnostic scatter-gather, routing and write path (asyncio).

Owner: A. Must not import ray: phase 1 and phase 2 only differ in the ShardTransport.
"""
