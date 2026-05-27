import asyncio
import os

from dotenv import load_dotenv
from obstore.store import ObjectStore, from_url

load_dotenv()

access_key = os.getenv("CEPH_ACCESS_KEY_ID")
secret_key = os.getenv("CEPH_SECRET_ACCESS_KEY")

s3_store: ObjectStore = from_url(
    "s3://intake-virtual-icechunk-store",
    config={
        "endpoint_url": "https://projects.pawsey.org.au",
        "access_key_id": access_key,
        "secret_access_key": secret_key,
    },
)


async def clean_store(
    s3_store: ObjectStore, prefix: str = "icecat", concurrency: int = 256
) -> None:
    sem = asyncio.Semaphore(concurrency)

    async def delete_one(path: str) -> None:
        """
        Concurrent deletion of a single object, with a semaphore to limit concurrency.
        """
        async with sem:
            await s3_store.delete_async(path)

    tasks = []

    async for batch in s3_store.list_async():
        for obj in batch:
            path = obj["path"]
            if path.startswith(prefix):
                tasks.append(asyncio.create_task(delete_one(path)))

        # periodically drain so to avoid building up too many tasks in memory
        if len(tasks) >= 1000:
            await asyncio.gather(*tasks)
            tasks.clear()

    if tasks:
        await asyncio.gather(*tasks)
