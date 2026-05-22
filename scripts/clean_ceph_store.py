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

del_list = []
for batch in s3_store.list():
    for obj in batch:
        if obj.get("path").startswith("icecat"):
            del_list.append(obj.get("path"))

for key in del_list:
    s3_store.delete(key)
