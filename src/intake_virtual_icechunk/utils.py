"""
Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
SPDX-License-Identifier: Apache-2.0
"""

import os
import posixpath
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


_VCC_SAFE_KWARGS: frozenset[str] = frozenset(
    {"endpoint_url", "endpoint", "allow_http", "region"}
)
"""
The set of keyword-argument names from ``store_options`` that are safe to
serialise into the catalog JSON sidecar for a virtual chunk container.  Only
non-credential, non-secret config (e.g. endpoint URL, HTTP flag) is included;
credentials must always come from the runtime environment.
"""


def _intake_cat_filename(store_path: Path | str) -> str:
    """
    Generate a JSON sidecar filename for an Icechunk store.

    The sidecar is named ``_intake_{store_name}.json``, where ``store_name`` is
    the stem of the store path (i.e. the last path component without its
    extension).  Uses ``posixpath`` so cloud URIs such as ``s3://bucket/my.icechunk``
    are handled correctly without mangling the ``://`` separator.

    Parameters
    ----------
    store_path : str
        The path or URI to the Icechunk store (e.g. ``/path/to/store.icechunk``
        or ``s3://bucket/store.icechunk``).

    Returns
    -------
    str
        The generated sidecar filename (e.g. ``_intake_store.json``).
    """
    basename = posixpath.basename(str(store_path).rstrip("/"))
    stem = posixpath.splitext(basename)[0]
    return f"_intake_{stem}.json"


def _sidecar_url(store_path: Path | str) -> str:
    """
    Return the full URL / path of the JSON sidecar file for the given store.

    For local paths the result is an absolute POSIX path string; for cloud
    URIs (``s3://``, ``gs://``, ``az://``) the result is a cloud URL with the
    sidecar filename appended.  Unlike ``Path`` joining, this function never
    mangles the ``://`` separator in cloud URIs.

    Parameters
    ----------
    store_path : str or Path
        The path or URI to the Icechunk store.

    Returns
    -------
    str
    """
    store_str = str(store_path)
    parsed = urlparse(store_str)
    scheme = parsed.scheme
    fname = _intake_cat_filename(store_str)

    if scheme in ("", "file") or (len(scheme) == 1 and scheme.isalpha()):
        if scheme == "file":
            # file:// URI — string-join so that ``file:///path`` is not mangled
            # to ``file:/path`` by Path() on POSIX.
            return f"{store_str.rstrip('/')}/{fname}"
        else:
            # Bare local path (or Windows drive letter) — Path() is safe here.
            return str(Path(store_str).expanduser() / fname)
    else:
        # Cloud URI — avoid Path which collapses ``://`` to ``:/``.
        return f"{store_str.rstrip('/')}/{fname}"


def _resolve_vcc_store(url_prefix: str, store_options: dict) -> Any:
    """
    Resolve a ``url_prefix`` (as returned by :func:`_resolve_store`) to an
    ``icechunk.ObjectStoreConfig`` suitable for use in a
    ``VirtualChunkContainer``.

    Only non-credential keys from *store_options* are forwarded to the
    icechunk factory (see :data:`_VCC_SAFE_KWARGS`).  Credentials must be
    supplied separately via ``icechunk.containers_credentials`` at runtime.

    Parameters
    ----------
    url_prefix : str
        The canonical URL prefix for the source data, e.g. ``"file:///g/data/"``,
        ``"s3://my-bucket/"``.
    store_options : dict
        Options for the object-store backend.  Non-credential keys (endpoint
        URL, ``allow_http``, ``region``) are forwarded; credential keys are
        ignored.

    Returns
    -------
    icechunk.ObjectStoreConfig
    """
    safe_opts = {k: v for k, v in store_options.items() if k in _VCC_SAFE_KWARGS}

    parsed = urlparse(url_prefix)
    scheme = parsed.scheme

    if scheme in ("", "file") or (len(scheme) == 1 and scheme.isalpha()):
        path = parsed.path if scheme == "file" else url_prefix
        return icechunk.local_filesystem_store(path)

    elif scheme == "s3":
        return icechunk.s3_store(url_prefix, **safe_opts)

    elif scheme in ("gs", "gcs"):
        raise NotImplementedError(
            "GCS virtual chunk containers are not yet supported. "
            "Please open an issue if you need this."
        )

    elif scheme in ("az", "abfs"):
        raise NotImplementedError(
            "Azure virtual chunk containers are not yet supported. "
            "Please open an issue if you need this."
        )

    raise ObjectStoreError(
        f"Unsupported URL prefix scheme: {scheme!r}. "
        "Expected a local path, file://, or s3://."
    )
