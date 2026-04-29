"""
Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
SPDX-License-Identifier: Apache-2.0
"""

import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import icechunk
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import AzureStore, GCSStore, LocalStore, S3Store

__stores__ = ["LocalStore", "S3Store", "GCSStore", "AzureStore"]


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


def _resolve_store(
    paths: str | list[str], store_options: dict
) -> tuple[ObjectStoreRegistry[Any], str]:
    """
    Virtualizarr requires us to create an obstore registry for the source data.
    This function resolves the catalog's asset paths to an appropriate obstore
    Store, wraps it in a registry, and returns both the registry and the
    canonical url_prefix used as the registry key.

    Returns
    -------
    tuple[ObjectStoreRegistry, str]
        The registry and the url_prefix string (e.g. ``"file:///g/data/p73"``
        or ``"s3://my-bucket"``).  Pass the prefix straight to
        ``icechunk.VirtualChunkContainer``.
    """
    paths = [paths] if isinstance(paths, str) else paths

    parsed = urlparse(paths[0])
    scheme = parsed.scheme

    if scheme in ("", "file") or (len(scheme) == 1 and scheme.isalpha()):
        # Normalise all paths to bare POSIX paths so commonpath works uniformly.
        local_paths = [
            urlparse(p).path if urlparse(p).scheme == "file" else os.path.abspath(p)
            for p in paths
        ]
        common = os.path.commonpath(local_paths)
        # Ensure trailing slash so icechunk accepts it as a directory prefix.
        if not common.endswith("/"):
            common = common + "/"
        url_prefix = f"file://{common}"
        store = LocalStore.from_url(url_prefix)
        return ObjectStoreRegistry({url_prefix: store}), url_prefix

    bucket = parsed.netloc

    if scheme == "s3":
        url_prefix = f"s3://{bucket}/"
        store = S3Store.from_url(  # type: ignore[assignment]
            f"{bucket}",
            endpoint=store_options.get("endpoint", None),
            access_key_id=store_options.get("access_key_id", None),
            secret_access_key=store_options.get("secret_access_key", None),
        )
        return ObjectStoreRegistry({url_prefix: store}), url_prefix
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


def _intake_cat_filename(store_path: Path | str) -> str:
    """
    Generate a JSON sidecar filename for an Icechunk store.

    The sidecar is named ``_intake_{store_name}.json``, where ``store_name`` is
    the stem of the store path (i.e. the filename without extension).  This
    ensures that the sidecar is easily identifiable and avoids potential
    conflicts with other files in the same directory.

    Parameters
    ----------
    store_path : str
        The path to the Icechunk store (e.g. ``/path/to/store.icechunk``).

    Returns
    -------
    str
        The generated sidecar filename (e.g. ``_intake_store.json``).
    """

    store_path_obj = Path(store_path)
    return f"_intake_{store_path_obj.stem}.json"
