from __future__ import annotations

import abc
from collections.abc import Generator, Iterable, Mapping
from functools import cached_property

# Copyright 2026 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from typing import TYPE_CHECKING, Any

import icechunk
import intake
import pandas as pd
import polars as pl
import xarray as xr
import zarr
from icechunk.xarray import to_icechunk
from intake_esm.core import esm_datastore
from intake_esm.utils import MinimalExploder
from obstore.store import from_url as _obs_from_url
from virtualizarr import open_virtual_dataset, open_virtual_mfdataset

from intake_virtual_icechunk.source._containers import VirtualChunkContainerModel
from intake_virtual_icechunk.source.utils import (
    DataStoreStructure,
    GroupEntry,
    ParserInferenceError,
)
from intake_virtual_icechunk.utils import (
    _filter_config_args,
    _intake_cat_filename,
    _path_to_url,
    _resolve_storage,
    _resolve_store,
    _resolve_vcc_store,
)

if TYPE_CHECKING:
    from obspec_utils.registry import ObjectStoreRegistry
    from obstore.store import ObjectStore
    from virtualizarr.parsers import (
        DMRPPParser,
        FITSParser,
        HDFParser,
        KerchunkJSONParser,
        KerchunkParquetParser,
        NetCDF3Parser,
        ZarrParser,
    )

    VirtualizarrParser = (
        type[DMRPPParser]
        | type[FITSParser]
        | type[HDFParser]
        | type[KerchunkJSONParser]
        | type[KerchunkParquetParser]
        | type[NetCDF3Parser]
        | type[ZarrParser]
    )


class AbstractIcechunkStoreBuilder(abc.ABC):
    """Abstract base class for building Icechunk stores from intake-esm catalogs.

    Concrete subclasses implement :meth:`build` to choose *how* source data
    lands in the store:

    * :class:`VirtualIcechunkStoreBuilder` — creates virtual references via
      VirtualiZarr; source data is never moved.
    * :class:`ZarrIcechunkStoreBuilder` — copies real data chunks into the
      Icechunk store via ``icechunk.xarray.to_icechunk``.

    Subclasses share catalogue discovery, metadata attachment, and sidecar
    serialisation logic defined here.

    Parameters
    ----------
    esm_datastore_path : Path or str
        Path to an existing intake-esm catalog JSON file. Stored internally as
        a string.
    icechunk_store_path : Path or str
        Path or URI at which to create the Icechunk store. Supported schemes:
        local path, ``s3://``, ``gs://`` / ``gcs://``, ``az://``.
    esm_datastore_kwargs : dict, optional
        Keyword arguments forwarded to ``intake.open_esm_datastore``.
    icechunk_storage_options : dict, optional
        Keyword arguments forwarded to the Icechunk storage backend for the
        target store. See :func:`intake_virtual_icechunk.utils._resolve_storage`.
    drop_cols : list[str], optional
        Column names in the intake-esm catalog's assets dataframe to omit from
        attached Zarr group metadata. The asset path column is always omitted.
    cols_to_deiter : list[str], optional
        Columns whose deduplicated iterable metadata should be stored as a
        scalar by taking the first value.
    xarray_kwargs: list[dict] | dict | None
        Passed to virtualizarr.open_virtual_mfdataset/open_virtual_dataset. If
        a list of dicts is passed, it must be the same length as the number of
        datasets in the datastore, and will be applied per dataset. If a single
        dict is passed, the same args will be passed to each dataset. If None,
        default arguments will be used.

    """

    def __init__(
        self,
        *,
        esm_datastore_path: Path | str,
        icechunk_store_path: Path | str,
        esm_datastore_kwargs: dict | None = None,
        icechunk_storage_options: dict | None = None,
        drop_cols: list[str] | None = None,
        cols_to_deiter: list[str] | None = None,
        xarray_kwargs: list[dict] | dict | None = None,
    ):
        self.esm_datastore_path = str(esm_datastore_path)
        self.esm_datastore_kwargs = esm_datastore_kwargs or {}

        self.store_path = str(icechunk_store_path)
        self._esm_ds: esm_datastore | None = None

        self.storage_options = icechunk_storage_options or {}
        self.drop_cols = drop_cols or []
        self.cols_to_deiter = cols_to_deiter or []
        self._xarray_kwargs = xarray_kwargs or {}

    @property
    def esm_ds(self) -> esm_datastore:
        """Lazily open and cache the intake-esm datastore."""

        if self._esm_ds is None:
            self._esm_ds = intake.open_esm_datastore(  # type: ignore[assignment]
                self.esm_datastore_path, **self.esm_datastore_kwargs
            )
        return self._esm_ds  # type: ignore[union-attr]

    @cached_property
    def xarray_kwargs(self) -> list[dict]:
        """Return user defined xarray kwargs, promoted to a list the same length as the datastore

        TODO: This should probably be a dict, not a list, because IDK if the datastore iteration is
        stable.
        """
        if isinstance(self._xarray_kwargs, dict):
            return [self._xarray_kwargs for _ in self.esm_ds]
        return self._xarray_kwargs

    def _extract_datastore_structure(self) -> DataStoreStructure:
        """Extract the grouping columns and asset column from the datastore.

        These fields drive both the physical group layout written into the
        Icechunk repository and the searchable metadata attached to each group.
        """
        esmcat = self.esm_ds.esmcat
        agg_control = esmcat.aggregation_control
        groupby_attrs = (
            agg_control.groupby_attrs if agg_control else list(esmcat.df.columns)
        )
        assets_col = esmcat.assets.column_name
        return DataStoreStructure(groupby_attrs, assets_col)

    def _prepare_group_iteration(self) -> DataStoreStructure:
        """Prepare the builder for iterating over intake-esm dataset groups."""

        structure = self._extract_datastore_structure()
        self.drop_cols = list(set(self.drop_cols + [structure.assets_col]))
        return structure

    def _iter_esm_groups(self) -> Generator[GroupEntry, None, None]:
        """Yield one logical dataset-group entry from the intake-esm catalog.

        Xarray opening kwargs will be folded in for each entry.
        """

        esmcat = self.esm_ds.esmcat
        structure = self._prepare_group_iteration()
        grouped = esmcat.grouped

        for (public_key, internal_key), kwargs in zip(
            esmcat._construct_group_keys().items(), self.xarray_kwargs
        ):
            group_df: pd.DataFrame = grouped.get_group(internal_key)
            yield GroupEntry.from_esm_group(
                public_key=public_key,
                group_df=group_df,
                groupby_attrs=structure.groupby_attrs,
                assets_col=structure.assets_col,
                xarray_kwargs=kwargs,
            )

    @abc.abstractmethod
    def build(self) -> None:
        """Build the Icechunk store from the intake-esm catalog."""
        ...

    @abc.abstractmethod
    def _write_entry(self, store: icechunk.IcechunkStore, entry: GroupEntry) -> None:
        """Materialize one entry into an open Icechunk transaction."""
        ...

    @abc.abstractmethod
    def _entry_action_verb(self) -> str:
        """Return the verb used in builder progress/error messages."""
        ...

    def _build_from_entries(
        self,
        repo: icechunk.Repository,
        entries: Iterable[GroupEntry],
        *,
        message: str,
    ) -> None:
        """Write an entry iterable into a repo transaction using this builder's path."""

        self.failed_list = []

        with repo.transaction("main", message=message) as store:
            for entry in entries:
                try:
                    self._write_entry(store, entry)
                except Exception as e:
                    self.failed_list.append((entry.public_key, e))
                    print(
                        f"Failed to {self._entry_action_verb()} group "
                        f"{entry.public_key}: {e}"
                    )

    @staticmethod
    def _is_concat_dim_order_error(exc: Exception) -> bool:
        """Return True when the exception is the xarray concat-order fallback case."""

        return (
            "Could not find any dimension coordinates to use to order the "
            "Dataset objects for concatenation" in str(exc)
        )

    def _attach_entry_metadata(
        self,
        zarr_group: zarr.Group,
        entry: GroupEntry,
    ) -> None:
        """Attach metadata from a builder entry to a written Zarr group."""

        if entry.has_metadata_df:
            self._attach_catalog_metadata(
                zarr_group,
                entry.group_df,
                entry.group_attrs,
            )
            return

        self._attach_group_attrs(zarr_group, entry.group_attrs)

    def _attach_group_attrs(
        self,
        zarr_group: zarr.Group,
        group_attrs: dict[str, Any],
    ) -> None:
        """Attach reduced metadata when only entry-level attrs are available."""

        filtered_attrs = {
            key: value
            for key, value in group_attrs.items()
            if key not in self.drop_cols
        }
        zarr_group.attrs.update(filtered_attrs)

    def _attach_catalog_metadata(
        self,
        zarr_group: zarr.Group,
        group_df: pd.DataFrame,
        group_attrs: dict,
    ) -> None:
        """
        Attach searchable intake-esm metadata to a Zarr group.

        Metadata is deduplicated per group, optional columns are dropped, and
        configured iterable columns are collapsed to scalars. Values from the
        exploded catalog metadata take precedence over ``group_attrs`` so richer
        per-asset metadata is preserved where both sources provide a value.

        Any remaining columns that still are not represented in ``zarr_group``
        attributes are backfilled from the first row of the grouped dataframe.
        """
        group_df = group_df.drop(columns=self.drop_cols, errors="ignore")

        exploded_metadata: Mapping[str, list[Any] | None] = (
            MinimalExploder(pl.from_pandas(group_df))()
            .unique()
            .to_dict(as_series=False)
        )

        # No None type until we deiter columns
        exploded_metadata = {
            k: [val for val in set(v) if val]  # type: ignore[arg-type]
            for k, v in exploded_metadata.items()
        }

        for col in self.cols_to_deiter:
            try:
                exploded_metadata[col] = exploded_metadata.get(col, [None])[0]  # type: ignore[index]
            except IndexError:
                exploded_metadata[col] = None

        exploded_metadata = {
            k: v for k, v in exploded_metadata.items() if k not in self.drop_cols
        }

        # Do exploded metadata first so it takes precedence over the groupby attrs,
        # which are more likely to have unimportant comments from NCO, etc.
        # TODO: Figure out if we can keep the order stable?

        group_attrs = {
            k: v
            for k, v in group_attrs.items()
            if k not in exploded_metadata and k not in self.drop_cols
        }

        zarr_group.attrs.update(exploded_metadata)
        zarr_group.attrs.update(group_attrs)

        for column in group_df.columns:
            if column not in zarr_group.attrs:
                # This should never actualy execute.
                zarr_group.attrs[column] = group_df[column].iloc[0]  # pragma: no cover


class VirtualIcechunkStoreBuilder(AbstractIcechunkStoreBuilder):
    """Build a virtual Icechunk store from an existing intake-esm datastore.

    Given a pre-built intake-esm catalog, this builder iterates over every
    dataset group in the catalog, opens the constituent files with VirtualiZarr
    to create virtual references, and writes each dataset as a named Zarr
    *group* inside a single Icechunk store. The result is one Icechunk store
    with one group per dataset, mirroring the logical grouping defined by the
    intake-esm catalog.

    Each group's ``.zattrs`` is populated with the ``groupby_attrs`` values for
    that dataset, so that :class:`~intake_virtual_icechunk.core.IcechunkCatalog`
    can discover, list, and search entries without reading any data arrays.

    After the store is built, a lightweight JSON sidecar is written into the
    store directory so the catalog can be re-opened from the store path or with
    :meth:`~intake_virtual_icechunk.core.IcechunkCatalog.from_json`.

    ``icechunk_storage_options`` configures the target Icechunk store.
    ``icechunk_store_options`` configures access to the source data referenced
    by the intake-esm catalog and is also filtered before being serialised into
    the virtual chunk container sidecar metadata.

    Parameters
    ----------
    esm_datastore_path : Path or str
        Path to an existing intake-esm catalog JSON file. Stored internally as
        a string.
    icechunk_store_path : Path or str
        Path or URI at which to create the Icechunk store. Supported schemes:
        local path, ``s3://``, ``gs://`` / ``gcs://``, ``az://``.
    esm_datastore_kwargs : dict, optional
        Keyword arguments forwarded to ``intake.open_esm_datastore``.
    parser : VirtualizarrParser, optional
        VirtualiZarr parser class to use when opening source assets. If not
        provided, the builder infers a parser from the intake-esm catalog's
        ``assets.format`` field.
    icechunk_storage_options : dict, optional
        Keyword arguments forwarded to the Icechunk storage backend for the
        target store. See :func:`intake_virtual_icechunk.utils._resolve_storage`.
    icechunk_store_options : dict, optional
        Keyword arguments used when opening the source data object store and
        reconstructing the virtual chunk container. Credential-like options are
        not written into the JSON sidecar.
    drop_cols : list[str], optional
        Column names in the intake-esm catalog's assets dataframe to omit from
        attached Zarr group metadata. The asset path column is always omitted.
    cols_to_deiter : list[str], optional
        Columns whose deduplicated iterable metadata should be stored as a
        scalar by taking the first value.
    xarray_kwargs: list[dict] | dict | None
        Passed to virtualizarr.open_virtual_mfdataset/open_virtual_dataset. If
        a list of dicts is passed, it must be the same length as the number of
        datasets in the datastore, and will be applied per dataset. If a single
        dict is passed, the same args will be passed to each dataset. If None,
        default arguments will be used.

    """

    def __init__(
        self,
        *,
        esm_datastore_path: Path | str,
        icechunk_store_path: Path | str,
        esm_datastore_kwargs: dict | None = None,
        parser: VirtualizarrParser | None = None,
        icechunk_storage_options: dict | None = None,
        icechunk_store_options: dict | None = None,
        drop_cols: list[str] | None = None,
        cols_to_deiter: list[str] | None = None,
        xarray_kwargs: list[dict] | dict | None = None,
    ):
        """Initialise a builder for one intake-esm -> virtual Icechunk conversion.

        The parser is instantiated eagerly. If ``parser`` is not provided, the
        builder infers a VirtualiZarr parser class from the intake-esm catalog's
        declared asset format and then instantiates it.
        """
        super().__init__(
            esm_datastore_path=esm_datastore_path,
            icechunk_store_path=icechunk_store_path,
            esm_datastore_kwargs=esm_datastore_kwargs,
            icechunk_storage_options=icechunk_storage_options,
            drop_cols=drop_cols,
            cols_to_deiter=cols_to_deiter,
            xarray_kwargs=xarray_kwargs,
        )
        self.store_options = icechunk_store_options or {}
        parser = parser or self._infer_parser()
        self.parser = parser()

    def __repr__(self) -> str:
        """Return a multiline representation showing the builder configuration."""
        return (
            "VirtualIcechunkStoreBuilder("
            f"\n\tesm_datastore_path='{self.esm_datastore_path}', "
            f"\n\ticechunk_store_path='{self.store_path}', "
            f"\n\tparser={self.parser.__class__.__name__}, "
            f"\n\tstorage_options={self.storage_options}, "
            f"\n\tstore_options={self.store_options}, "
            f"\n\tdrop_cols={self.drop_cols}, "
            f"\n\tcols_to_deiter={self.cols_to_deiter}"
            f"\n\txarray_kwargs={self._xarray_kwargs},"
            "\n)"
        )

    def _infer_parser(self) -> VirtualizarrParser:
        """
        Infer the VirtualiZarr parser class from the intake-esm assets format.

        Calling this method does not set ``self.parser``. It returns a parser
        class, not an instance.
        """
        from virtualizarr import parsers

        try:
            format = self.esm_ds.esmcat.assets.format.value  # type: ignore[union-attr]
        except AttributeError:
            raise ParserInferenceError(
                "Cannot infer parser from intake-esm catalog: "
                "the 'format' field in the catalog's 'assets' specification is missing. "
                "Only assets with a specified 'format' can be built into an Icechunk store. "
                "See https://intake-esm.readthedocs.io/en/stable/reference/esm-catalog-spec.html#assets-object"
            )

        if format is None:
            raise ParserInferenceError(
                "Cannot infer parser from intake-esm catalog: "
                "the 'format' field in the catalog's 'assets' specification is missing."
                "Only assets with a specified 'format' can be built into an Icechunk store."
                "See https://intake-esm.readthedocs.io/en/stable/reference/esm-catalog-spec.html#assets-object"
            )

        PARSER_MAP: dict[str, VirtualizarrParser] = {
            "netcdf": parsers.HDFParser,
            "zarr": parsers.ZarrParser,
            "zarr2": parsers.ZarrParser,
            "zarr3": parsers.ZarrParser,
            "reference": parsers.KerchunkJSONParser,
            # Don't know the best way to handle things that overlap in the format but have different parsers yet.
            # "todo_0": parsers.DMRPPParser,
            # "todo_1": parsers.FITSParser,
            # "todo_2": parsers.NetCDF3Parser,
            # "todo_4": parsers.KerchunkParquetParser,
        }

        try:
            return PARSER_MAP[format]
        except KeyError:
            raise ParserInferenceError(
                f"Unsupported parser format '{format}' specified in intake-esm catalog. "
                f"Supported formats are: {list(PARSER_MAP.keys())}."
            )

    def _create_registry(self) -> ObjectStoreRegistry:
        """
        Create and cache the ObjectStoreRegistry used to read source assets.

        The registry and source URL prefix are inferred from the intake-esm
        assets column and ``self.store_options``.
        """
        path_column = self.esm_ds.esmcat.assets.column_name
        paths = self.esm_ds.esmcat.df[path_column].tolist()
        self.obstore_registry, self.source_url_prefix = _resolve_store(
            paths, self.store_options
        )

        return self.obstore_registry

    def _entry_action_verb(self) -> str:
        """Return the verb used in progress/error messages for this builder."""

        return "virtualise"

    def _write_entry(self, store: icechunk.IcechunkStore, entry: GroupEntry) -> None:
        """Write one virtualized group into an open Icechunk transaction."""

        # Are these defaults too opinionated?  Maybe we should have a narrower set
        # of defaults or have a total override if the user provides any xarray kwargs?
        default_kwargs = dict(
            parser=self.parser,
            registry=self.obstore_registry,
            parallel="dask",
            decode_times=False,
            coords="minimal",
            compat="override",
        )

        open_kwargs = {**default_kwargs, **entry.xarray_kwargs}
        try:
            with open_virtual_mfdataset(urls=entry.file_paths, **open_kwargs) as vds:
                vds.vz.to_icechunk(store, group=entry.public_key)
        except Exception as exc:
            if not self._is_concat_dim_order_error(exc):
                raise exc

            open_kwargs = _filter_kwargs(open_kwargs)

            with open_virtual_dataset(url=entry.file_paths[0], **open_kwargs) as vds:
                vds.vz.to_icechunk(store, group=entry.public_key)

        # Write group metadata into .zattrs so the catalog can search
        # these groups without opening the arrays.
        # We also want to attach metadata from the intake-esm catalog
        # at this point. We don't need to attach things like variable
        # names, because we can get those direct from the groups, but
        # things like 'frequency', etc. will need to be included.

        # To keep life simple, we'll just attach everything, and then
        # compute variables, coordinates, dimensions etc. on the fly later
        zarr_group = zarr.open_group(store, path=entry.public_key, mode="a")
        # Would make more sense to merge group_attrs and esm_ds_metadata
        # first in a sensible way
        self._attach_entry_metadata(zarr_group, entry)

        print(f"Virtualised group {entry.public_key} successfully!")

    def build(self) -> None:
        """Build the virtual Icechunk store.

        For each dataset group in the intake-esm catalog:

        1. Collects the asset file paths that belong to the group.
        2. Opens each file with VirtualiZarr to create virtual references.
        3. Combines and writes the virtual dataset as a named Zarr group in
           the Icechunk store.
        4. Writes the group's ``groupby_attrs`` values into ``.zattrs``.

        After all groups are written, a JSON sidecar is saved alongside the
        store for use with
        :meth:`~intake_virtual_icechunk.core.IcechunkCatalog.from_json`.

        The group name for each dataset is derived from the catalog's
        ``groupby_attrs``, so the Icechunk store structure mirrors the
        intake-esm grouping.

        Notes
        -----
        The current implementation records per-group failures in
        ``self.failed_list`` and continues building the remaining groups.
        It also persists a JSON sidecar inside the target store so the catalog
        can be reopened later without re-supplying virtual chunk configuration.
        """
        from intake_virtual_icechunk.cat import VirtualIcechunkCatalogModel

        # Resolve registry first so self.source_url_prefix is available for the
        # VirtualChunkContainer config below.
        self._create_registry()

        storage = _resolve_storage(self.store_path, self.storage_options)

        self.vc_container = icechunk.VirtualChunkContainer(
            url_prefix=self.source_url_prefix,
            store=_resolve_vcc_store(self.source_url_prefix, self.store_options),
        )

        config = icechunk.RepositoryConfig.default()
        config.set_virtual_chunk_container(self.vc_container)

        credentials = icechunk.containers_credentials({self.source_url_prefix: None})
        repo = icechunk.Repository.create(storage, config, credentials)

        # Persist the configuration so we don't need to figure it out when we come
        # back to open the store
        repo.save_config()

        # Now, we need to persist the virtual chunk containers so that we can
        # reinstantiate them when we open the store later, to avoid the 'safe by
        # default' behaviour.
        # self.vc_containers

        self._build_from_entries(
            repo,
            self._iter_esm_groups(),
            message=f"Build Virtual Icechunk catalog for {self.esm_ds.name}",
        )

        # Write the JSON sidecar inside the store directory
        sidecar_fname = _intake_cat_filename(self.store_path)

        model = VirtualIcechunkCatalogModel(
            store=self.store_path,
            storage_options=self.storage_options,
            virtual_chunk_model=VirtualChunkContainerModel.from_virtual_chunk_container(
                self.vc_container,
                store_options=self.store_options,
            ),
        )
        sidecar_store: ObjectStore = _obs_from_url(
            _path_to_url(self.store_path),
            config=_filter_config_args(self.storage_options),
        )
        model.save(sidecar_fname, store=sidecar_store)


class IcechunkStoreBuilder(AbstractIcechunkStoreBuilder):
    """Build a real Icechunk store by copying data from an intake-esm datastore.

    Given a pre-built intake-esm catalog, this builder iterates over every
    dataset group in the catalog, opens the constituent files with
    ``xarray.open_mfdataset``, and writes each dataset as a named Zarr
    *group* inside a single Icechunk store. Data chunks are **copied** into
    the Icechunk store, so source files do not need to remain accessible once
    the build is complete.

    The resulting store requires no virtual chunk container configuration: it
    can be opened with :class:`~intake_virtual_icechunk.core.IcechunkCatalog`
    directly, without supplying any source-data credentials.

    Parameters
    ----------
    esm_datastore_path : Path or str
        Path to an existing intake-esm catalog JSON file. Stored internally as
        a string.
    icechunk_store_path : Path or str
        Path or URI at which to create the Icechunk store. Supported schemes:
        local path, ``s3://``, ``gs://`` / ``gcs://``, ``az://``.
    esm_datastore_kwargs : dict, optional
        Keyword arguments forwarded to ``intake.open_esm_datastore``.
    icechunk_storage_options : dict, optional
        Keyword arguments forwarded to the Icechunk storage backend for the
        target store. See :func:`intake_virtual_icechunk.utils._resolve_storage`.
    xarray_kwargs : dict, optional
        Keyword arguments forwarded to ``xarray.open_mfdataset`` when reading
        each group's source files (e.g. ``{'decode_times': False}``).
    drop_cols : list[str], optional
        Column names in the intake-esm catalog's assets dataframe to omit from
        attached Zarr group metadata. The asset path column is always omitted.
    cols_to_deiter : list[str], optional
        Columns whose deduplicated iterable metadata should be stored as a
        scalar by taking the first value.
    xarray_kwargs: list[dict] | dict | None
        Passed to xarray.open_mfdataset/open_dataset. If a list of dicts is passed,
        it must be the same length as the number of datasets in the datastore,
        and will be applied per dataset. If a single dict is passed, the same
        args will be passed to each dataset. If None, default arguments will be
        used. Combine related kwargs are dropped if passed to open_dataset.
    """

    def __init__(
        self,
        *,
        esm_datastore_path: Path | str,
        icechunk_store_path: Path | str,
        esm_datastore_kwargs: dict | None = None,
        icechunk_storage_options: dict | None = None,
        xarray_kwargs: list[dict] | dict | None = None,
        drop_cols: list[str] | None = None,
        cols_to_deiter: list[str] | None = None,
    ):
        """Initialise a builder that copies real data into the Icechunk store."""
        super().__init__(
            esm_datastore_path=esm_datastore_path,
            icechunk_store_path=icechunk_store_path,
            esm_datastore_kwargs=esm_datastore_kwargs,
            icechunk_storage_options=icechunk_storage_options,
            drop_cols=drop_cols,
            cols_to_deiter=cols_to_deiter,
            xarray_kwargs=xarray_kwargs,
        )

    def __repr__(self) -> str:
        """Return a multiline representation showing the builder configuration."""
        return (
            "ZarrIcechunkStoreBuilder("
            f"\n\tesm_datastore_path='{self.esm_datastore_path}', "
            f"\n\ticechunk_store_path='{self.store_path}', "
            f"\n\txarray_kwargs={self._xarray_kwargs}, "
            f"\n\tstorage_options={self.storage_options}, "
            f"\n\tdrop_cols={self.drop_cols}, "
            f"\n\tcols_to_deiter={self.cols_to_deiter}"
            "\n)"
        )

    def _entry_action_verb(self) -> str:
        """Return the verb used in progress/error messages for this builder."""

        return "write"

    def _write_entry(self, store: icechunk.IcechunkStore, entry: GroupEntry) -> None:
        """Write one real-data group into an open Icechunk transaction."""

        try:
            with xr.open_mfdataset(entry.file_paths, **entry.xarray_kwargs) as ds:
                to_icechunk(ds, store.session, group=entry.public_key, mode="a")
        except Exception as exc:
            if not self._is_concat_dim_order_error(exc):
                raise exc

            kwargs = _filter_kwargs(entry.xarray_kwargs)
            with xr.open_dataset(
                entry.file_paths[0],
                **kwargs,
            ) as ds:
                to_icechunk(
                    ds,
                    store.session,
                    group=entry.public_key,
                    mode="a",
                )

        # Write group metadata into .zattrs so the catalog can search
        # these groups without opening the arrays.
        zarr_group = zarr.open_group(store, path=entry.public_key, mode="a")
        self._attach_entry_metadata(zarr_group, entry)

        print(f"Wrote group {entry.public_key} successfully!")

    def build(self) -> None:
        """Build the Icechunk store by copying real data from the source assets.

        For each dataset group in the intake-esm catalog:

        1. Collects the asset file paths that belong to the group.
        2. Opens the files with ``xarray.open_mfdataset``.
        3. Writes the dataset as a named Zarr group in the Icechunk store.
        4. Writes the group's ``groupby_attrs`` values into ``.zattrs``.

        After all groups are written, a JSON sidecar is saved alongside the
        store for use with
        :meth:`~intake_virtual_icechunk.core.IcechunkCatalog.from_json`.
        The sidecar does **not** contain a virtual chunk container entry
        because the store holds real data chunks.

        Notes
        -----
        Like the virtual builder path, this implementation records per-group
        failures in ``self.failed_list`` and continues building the remaining
        groups.
        """
        from intake_virtual_icechunk.cat import VirtualIcechunkCatalogModel

        storage = _resolve_storage(self.store_path, self.storage_options)

        config = icechunk.RepositoryConfig.default()
        repo = icechunk.Repository.create(storage, config)
        repo.save_config()

        self._build_from_entries(
            repo,
            self._iter_esm_groups(),
            message=f"Build Icechunk catalog for {self.esm_ds.name}",
        )

        # Write the JSON sidecar inside the store directory
        sidecar_fname = _intake_cat_filename(self.store_path)

        model = VirtualIcechunkCatalogModel(
            store=self.store_path,
            storage_options=self.storage_options,
            virtual_chunk_model=None,
        )
        sidecar_store: ObjectStore = _obs_from_url(
            _path_to_url(self.store_path),
            config=_filter_config_args(self.storage_options),
        )
        model.save(sidecar_fname, store=sidecar_store)


def _filter_kwargs(kwargs: dict) -> dict:
    """Filter out mfdataset specific kwargs that would cause the single-dataset open to fail"""
    return {
        k: v
        for k, v in kwargs.items()
        if k
        not in [
            "parallel",
            "coords",
            "compat",
            "combine_attrs",
            "join",
            "concat_dim",
        ]
    }
