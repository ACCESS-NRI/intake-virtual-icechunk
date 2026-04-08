# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import pandas as pd
import zarr
from intake.catalog import Catalog

from .source import IcechunkDataSource


def _match_query(attrs: dict, query: dict) -> bool:
    """Return True if every key-value pair in *query* matches *attrs*.

    A query value may be a scalar or a list; a scalar is treated as a
    single-element list so that both ``search(x='a')`` and
    ``search(x=['a', 'b'])`` work uniformly.
    """
    for key, value in query.items():
        attr_val = attrs.get(key)
        allowed = value if isinstance(value, list) else [value]
        if attr_val not in allowed:
            return False
    return True


class IcechunkCatalog(Catalog):
    """
    An intake plugin for reading an Icechunk store built from an intake-esm catalog.

    The store contains one Zarr group per dataset, written by
    :class:`~intake_virtual_icechunk._build.IcechunkStoreBuilder`.  Per-entry
    metadata (the attributes used for searching) is stored in each group's
    ``.zattrs``.  This catalog mirrors the :class:`~intake_esm.esm_datastore`
    API so that switching between the two is straightforward.

    Registered as the ``virtual_icechunk`` intake driver, so it is accessible
    via ``intake.open_virtual_icechunk()``.

    Parameters
    ----------
    store : str
        Path or URI to the Icechunk store.  Supported schemes: local path,
        ``s3://``, ``gs://`` / ``gcs://``, ``az://``.
    storage_options : dict, optional
        Credential/config keyword arguments forwarded to the Icechunk storage
        backend (e.g. ``{'from_env': True}`` for S3).
    xarray_kwargs : dict, optional
        Keyword arguments forwarded to ``xarray.open_zarr()``.
    intake_kwargs : dict, optional
        Additional keyword arguments passed through to
        :py:class:`~intake.catalog.Catalog`.

    Examples
    --------
    Open a catalog saved by :class:`~intake_virtual_icechunk._build.IcechunkStoreBuilder`:

    >>> import intake
    >>> cat = intake.open_virtual_icechunk('/path/to/store')
    >>> cat.keys()
    ['CMIP.BCC.BCC-ESM1.historical', 'CMIP.BCC.BCC-ESM1.ssp585']
    >>> ds = cat['CMIP.BCC.BCC-ESM1.historical'].to_dask()

    Or load from a JSON sidecar:

    >>> cat = IcechunkCatalog.from_json('/path/to/catalog.json')
    """

    name = "virtual_icechunk"
    container = "xarray"

    def __init__(
        self, store, *, storage_options=None, xarray_kwargs=None, **intake_kwargs
    ):
        super().__init__(**intake_kwargs)
        self.store = store
        self.storage_options = storage_options or {}
        self.xarray_kwargs = xarray_kwargs or {}
        self._entries = {}
        self.datasets = {}
        self._allowed_keys = None  # None → all top-level groups from the store

        # Lazily-opened backend objects
        self._open_repo = None
        self._open_zarr_store = None
        self._open_root_group = None

    # ------------------------------------------------------------------
    # Lazy store access
    # ------------------------------------------------------------------

    @property
    def _repo(self):
        """Open (and cache) the icechunk Repository."""
        if self._open_repo is None:
            import icechunk

            from ._storage import _resolve_storage

            storage = _resolve_storage(self.store, self.storage_options)
            self._open_repo = icechunk.Repository.open(storage)
        return self._open_repo

    @property
    def _zarr_store(self):
        """Return a read-only zarr-compatible IcechunkStore for the main branch."""
        if self._open_zarr_store is None:
            self._open_zarr_store = self._repo.readonly_session("main").store
        return self._open_zarr_store

    @property
    def _root_group(self):
        """Return the root zarr Group for the store."""
        if self._open_root_group is None:
            self._open_root_group = zarr.open_group(self._zarr_store, mode="r")
        return self._open_root_group

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def _from_parent(
        cls, parent: IcechunkCatalog, allowed_keys: list[str]
    ) -> IcechunkCatalog:
        """Create a filtered catalog that reuses an already-opened store."""
        cat = cls(
            store=parent.store,
            storage_options=parent.storage_options,
            xarray_kwargs=parent.xarray_kwargs,
        )
        # Share the already-opened backend so we don't re-open the repo.
        cat._open_repo = parent._open_repo
        cat._open_zarr_store = parent._open_zarr_store
        cat._open_root_group = parent._open_root_group
        cat._allowed_keys = allowed_keys
        return cat

    @classmethod
    def from_json(
        cls,
        json_file: str,
        *,
        xarray_kwargs: dict | None = None,
        storage_options: dict | None = None,
    ) -> IcechunkCatalog:
        """
        Load an :class:`IcechunkCatalog` from a JSON sidecar file.

        Parameters
        ----------
        json_file : str
            Path or URL to the catalog JSON file produced by :meth:`save` or
            :class:`~intake_virtual_icechunk._build.IcechunkStoreBuilder`.
        xarray_kwargs : dict, optional
            Keyword arguments forwarded to ``xarray.open_zarr()``.
        storage_options : dict, optional
            fsspec options for *reading the JSON file itself* (not for the
            Icechunk store — those are embedded in the JSON).
        """
        from .cat import VirtualIcechunkCatalogModel

        model = VirtualIcechunkCatalogModel.load(
            json_file, storage_options=storage_options
        )
        return cls(
            store=model.store,
            storage_options=model.storage_options,
            xarray_kwargs=xarray_kwargs or {},
        )

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def save(
        self,
        name: str,
        *,
        directory: str | None = None,
        json_dump_kwargs: dict | None = None,
    ) -> None:
        """
        Save a JSON sidecar file pointing to this catalog's Icechunk store.

        Parameters
        ----------
        name : str
            Stem of the output file (without the ``.json`` extension).
        directory : str, optional
            Directory to write the file to. Defaults to the current working
            directory.
        json_dump_kwargs : dict, optional
            Additional keyword arguments forwarded to :func:`json.dump`.
        """
        from .cat import VirtualIcechunkCatalogModel

        model = VirtualIcechunkCatalogModel(
            store=self.store,
            storage_options=self.storage_options,
        )
        model.save(name, directory=directory, json_dump_kwargs=json_dump_kwargs)

    # ------------------------------------------------------------------
    # Core catalog interface
    # ------------------------------------------------------------------

    def keys(self) -> list[str]:
        """
        Get keys for the catalog entries (one per top-level Zarr group in the store).

        Returns
        -------
        list of str
            Group path keys, one per dataset written by
            :class:`~intake_virtual_icechunk._build.IcechunkStoreBuilder`.
            When this catalog is the result of :meth:`search`, only the
            matching keys are returned.
        """
        if self._allowed_keys is not None:
            return self._allowed_keys
        return [name for name, _ in self._root_group.groups()]

    def __len__(self):
        return len(self.keys())

    def __repr__(self):
        return f"<IcechunkCatalog with {len(self)} dataset(s) from {self.store!r}>"

    def _repr_html_(self):
        return (
            f"<p><strong>IcechunkCatalog with {len(self)} dataset(s)</strong> "
            f"from <code>{self.store}</code></p>"
        )

    def __getitem__(self, key: str) -> IcechunkDataSource:
        """
        Return an :class:`~intake_virtual_icechunk.source.IcechunkDataSource`
        for the given group key.

        Parameters
        ----------
        key : str
            Zarr group path within the store.

        Returns
        -------
        intake_virtual_icechunk.source.IcechunkDataSource

        Raises
        ------
        KeyError
            If *key* is not found in the store.
        """
        if key not in self._entries:
            if key not in self.keys():
                raise KeyError(key)
            self._entries[key] = IcechunkDataSource(
                key=key,
                store=self._zarr_store,
                group=key,
                storage_options=self.storage_options,
                xarray_kwargs=self.xarray_kwargs,
            )
        return self._entries[key]

    def __contains__(self, key):
        try:
            self[key]
        except KeyError:
            return False
        return True

    # ------------------------------------------------------------------
    # Search and metadata
    # ------------------------------------------------------------------

    def search(self, **query) -> IcechunkCatalog:
        """
        Search for entries in the catalog by matching group ``.zattrs``.

        Parameters
        ----------
        **query
            Each keyword maps to a ``.zattrs`` attribute name.  The value
            may be a scalar or a list of allowed values.

        Returns
        -------
        IcechunkCatalog
            A new catalog containing only the matching entries.  The
            underlying Icechunk store is shared — it is not re-opened.

        Examples
        --------
        >>> cat.search(source_id='BCC-ESM1')
        >>> cat.search(experiment_id=['historical', 'ssp585'])
        >>> cat.search(source_id='BCC-ESM1', experiment_id='historical')
        """
        if not query:
            return self

        matched = [
            key
            for key in self.keys()
            if _match_query(dict(self._root_group[key].attrs), query)
        ]
        return IcechunkCatalog._from_parent(self, matched)

    @property
    def df(self) -> pd.DataFrame:
        """
        Return a :class:`~pandas.DataFrame` of all catalog entry metadata.

        Each row corresponds to one Zarr group (catalog entry). The ``key``
        column holds the group path; remaining columns are drawn from each
        group's ``.zattrs`` as written by
        :class:`~intake_virtual_icechunk._build.IcechunkStoreBuilder`.
        """
        records = []
        for key in self.keys():
            row: dict = {"key": key}
            row.update(dict(self._root_group[key].attrs))
            records.append(row)
        return pd.DataFrame(records)

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def to_dataset_dict(
        self,
        xarray_kwargs: dict | None = None,
        progressbar: bool = True,
    ) -> dict:
        """
        Load catalog entries into a dictionary of xarray Datasets.

        Parameters
        ----------
        xarray_kwargs : dict, optional
            Keyword arguments forwarded to ``xarray.open_zarr()``.  Merged
            with (and taking precedence over) the *xarray_kwargs* supplied at
            construction time.
        progressbar : bool, optional
            If ``True``, display a progress bar while loading datasets.

        Returns
        -------
        dict of str -> xarray.Dataset
            One Dataset per catalog entry, keyed by the group path.
        """
        merged_kwargs = {**self.xarray_kwargs, **(xarray_kwargs or {})}
        keys = self.keys()

        if progressbar:
            try:
                from tqdm.auto import tqdm

                keys = tqdm(keys)
            except ImportError:
                pass

        result = {}
        for key in keys:
            source = IcechunkDataSource(
                key=key,
                store=self._zarr_store,
                group=key,
                storage_options=self.storage_options,
                xarray_kwargs=merged_kwargs,
            )
            result[key] = source.to_dask()
        return result

    def to_dask(self, **kwargs):
        """
        Return the catalog as a single xarray Dataset.

        Only valid when the catalog contains exactly one entry.

        Parameters
        ----------
        **kwargs
            Passed through to :meth:`to_dataset_dict`.

        Returns
        -------
        xarray.Dataset

        Raises
        ------
        ValueError
            If the catalog contains zero or more than one entry.
        """
        if len(self) != 1:
            raise ValueError(
                f"to_dask() requires exactly one catalog entry, but this catalog has {len(self)}. "
                "Use to_dataset_dict() instead."
            )
        res = self.to_dataset_dict(**{**kwargs, "progressbar": False})
        _, ds = res.popitem()
        return ds
