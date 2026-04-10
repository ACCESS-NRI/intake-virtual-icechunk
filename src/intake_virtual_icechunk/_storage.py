# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from urllib.parse import urlparse

import icechunk
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import LocalStore


class IceChunkStoreError(Exception):
    """Custom exception for errors related to Icechunk store resolution."""

    pass


class ObjectStoreError(Exception):
    """Custom exception for errors related to object store resolution."""

    pass


def _resolve_storage(store: str, storage_options: dict) -> icechunk.Storage:
    """
    Resolve a store path/URL to an ``icechunk.Storage`` object.

    Parameters
    ----------
    store : str
        The store URI.  Supported schemes:

        * Local path (including Windows paths such as ``C:/path/...``)
          or ``file://...`` → local filesystem storage.
        * ``s3://bucket/prefix`` → AWS S3 (or compatible) storage.
        * ``gs://bucket/prefix`` or ``gcs://bucket/prefix`` → Google Cloud Storage.
        * ``az://container/prefix`` → Azure Blob Storage.  The ``account``
          name must be supplied via *storage_options*.

    storage_options : dict
        Credential/config keyword arguments forwarded to the appropriate
        icechunk storage factory.  For example ``{'from_env': True}`` for S3
        or ``{'account': 'myaccount', 'from_env': True}`` for Azure.

    Returns
    -------
    icechunk.Storage
    """

    parsed = urlparse(store)
    scheme = parsed.scheme

    # On Windows, urlparse("C:/path/...") sets scheme="c" (the drive letter).
    # Treat single-character schemes as local paths.
    if scheme in ("", "file") or (len(scheme) == 1 and scheme.isalpha()):
        path = parsed.path if scheme == "file" else store
        return icechunk.local_filesystem_storage(path)

    elif scheme == "s3":
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        return icechunk.s3_storage(bucket=bucket, prefix=prefix, **storage_options)

    elif scheme in ("gs", "gcs"):
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        return icechunk.gcs_storage(bucket=bucket, prefix=prefix, **storage_options)

    elif scheme in ("az", "abfs"):
        container = parsed.netloc
        prefix = parsed.path.lstrip("/")
        return icechunk.azure_storage(
            container=container, prefix=prefix, **storage_options
        )

    raise IceChunkStoreError(
        f"Unsupported store scheme: {scheme!r}. "
        "Expected a local path or one of s3://, gs://, gcs://, az://."
    )


def _resolve_store(paths: str | list[str], store_options: dict) -> ObjectStoreRegistry:
    """
    Virtualizarr requires us to create an obstore registry for the source data.
    This function resolves the first path in the catalog's assets to an appropriate
    obstore Store, which we can then wrap in a registry and pass to Virtualizarr.
    """
    paths = [paths] if isinstance(paths, str) else paths

    path = paths[0]  # Just use the first for the time being (janky, idgaf)

    parsed = urlparse(path)
    scheme = parsed.scheme

    if scheme in ("", "file") or (len(scheme) == 1 and scheme.isalpha()):
        store = LocalStore(
            prefix=path,
        )
        return ObjectStoreRegistry({"path": store})

    bucket = parsed.netloc

    if scheme == "s3":
        store = S3Store.from_url(  # type: ignore[assignment]
            f"{bucket}",
            endpoint=store_options.get("endpoint", None),
            access_key_id=store_options.get("access_key_id", None),
            secret_access_key=store_options.get("secret_access_key", None),
        )
    elif scheme in ("gs", "gcs"):
        raise NotImplementedError(
            "GCS support is disabled until I figure out the correct initialisation params"
        )
        store = GCSStore.from_url(
            f"{bucket}",
            endpoint=store_options.get("endpoint", None),
            access_key_id=store_options.get("access_key_id", None),
            secret_access_key=store_options.get("secret_access_key", None),
        )
    elif scheme in ("az", "abfs"):
        raise NotImplementedError(
            "Azure support is disabled until I figure out the correct initialisation params"
        )
        return AzureStore.from_url(
            f"{bucket}",
            account=store_options.get("account", None),
            endpoint=store_options.get("endpoint", None),
            access_key_id=store_options.get("access_key_id", None),
            secret_access_key=store_options.get("secret_access_key", None),
        )

    return ObjectStoreRegistry({f"{bucket}": store})

    raise ObjectStoreError(
        f"Unsupported store scheme: {scheme!r}. "
        "Expected a local path or one of s3://, gs://, gcs://, az://."
    )
