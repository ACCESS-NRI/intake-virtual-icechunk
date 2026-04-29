# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any

import xarray as xr
from intake.source.base import DataSource, Schema


class IcechunkDataSourceError(Exception):
    pass


class IcechunkDataSource(DataSource):
    """An intake-compatible Data Source for a single Zarr group in an Icechunk store.

    This is the per-entry source returned by
    :class:`~intake_virtual_icechunk.core.IcechunkCatalog` when a key is looked
    up.  It mirrors :class:`~intake_esm.source.ESMDataSource` so the two plugins
    feel identical to callers.

    Parameters
    ----------
    key : str
        The catalog key / Zarr group path for this dataset.
    store : icechunk.IcechunkStore
        An already-opened, zarr-compatible ``IcechunkStore``.  Obtain one via
        ``IcechunkCatalog._zarr_store`` (or
        ``icechunk.Repository.open(...).readonly_session('main').store``).
        Passing a pre-opened store avoids re-opening the repository for every
        data source.
    group : str
        Zarr group path within the store to open.
    storage_options : dict, optional
        Retained for API compatibility; not used when *store* is already an
        ``IcechunkStore``.
    xarray_kwargs : dict, optional
        Keyword arguments forwarded to ``xarray.open_zarr()``.
    intake_kwargs : dict, optional
        Additional keyword arguments passed through to
        :py:class:`~intake.source.base.DataSource`.
    """

    version = "0.0.1"
    container = "xarray"
    name = "icechunk_datasource"
    partition_access = True

    def __init__(
        self,
        key: str,
        store: Any,
        group: str,
        *,
        storage_options: dict[str, Any] | None = None,
        xarray_kwargs: dict[str, Any] | None = None,
        intake_kwargs: dict[str, Any] | None = None,
    ) -> None:
        intake_kwargs = intake_kwargs or {}
        super().__init__(**intake_kwargs)
        self.key = key
        self.store = store
        self.group = group
        self.storage_options = storage_options or {}
        self.xarray_kwargs = xarray_kwargs or {}
        self._ds = None

    def __repr__(self) -> str:
        return f"<IcechunkDataSource (key: {self.key}, store: {self.store!r})>"

    @property
    def ds(self) -> xr.Dataset:
        """The xarray Dataset for this data source."""
        if self._ds is None:
            ds = self._open_dataset()
        return ds

    def _get_schema(self) -> Schema:
        if self._schema is None:  # type: ignore[has-type]
            metadata = {
                "dims": dict(self.ds.dims),
                "data_vars": list(self.ds.data_vars),
            }
            self._schema = Schema(
                datashape=None,
                dtype=None,
                shape=None,
                npartitions=1,
                extra_metadata=metadata,
            )
        return self._schema

    def _open_dataset(self) -> xr.Dataset:
        """Open the Zarr group from the Icechunk store as an xarray Dataset."""
        try:
            return xr.open_zarr(
                self.store,
                group=self.group,
                **self.xarray_kwargs,
            )
        except Exception as exc:
            raise IcechunkDataSourceError(
                f"Failed to load dataset with key='{self.key}' from store '{self.store}'"
            ) from exc

    def to_xarray(self) -> xr.Dataset:
        """Return the xarray Dataset (with dask-backed arrays)."""
        self._load_metadata()
        return self.ds

    def close(self) -> None:
        """Drop the open dataset from memory."""
        self._ds = None
        self._schema = None
