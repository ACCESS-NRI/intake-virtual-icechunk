# intake-virtual-icechunk

An intake plugin for building and reading [Icechunk](https://icechunk.io) stores via
[VirtualiZarr](https://virtualizarr.readthedocs.io) and
[intake-esm](https://intake-esm.readthedocs.io).

## Concept

The goal is a pipeline that takes a pre-built intake-esm datastore and produces a
single virtual Icechunk store that mirrors its structure:

1. Open a pre-built intake-esm datastore with intake-esm.
2. For each dataset in the catalog, open the constituent files with VirtualiZarr to
   create virtual references — no data is copied.
3. Write each dataset as a named **Zarr group** inside one Icechunk store, using the
   catalog's `groupby_attrs` to derive the group name.
4. Expose the result through an intake driver (`virtual_icechunk`) that hides all
   Icechunk-specific complexity (sessions, stores, branches) behind an interface that
   feels like a hybrid of an esm-datastore and an `xarray.Dataset` — defaulting to
   Xarray semantics wherever possible, and falling back to esm-datastore conventions
   only where necessary (e.g. catalog search and group selection).

The end result is one Icechunk store, one group per dataset, fully virtual (no data
duplication), and accessible via `intake.open_virtual_icechunk()`.

## This package provides two things

1. **Building**
   - `VirtualIcechunkStoreBuilder` — builds a virtual Icechunk store from a pre-built
     intake-esm catalog without copying source data.
   - `IcechunkStoreBuilder` — builds a real-data Icechunk store from a pre-built
     intake-esm catalog by copying chunks into Icechunk.
2. **Reading** (`IcechunkSource`) — an intake driver for opening a group from an Icechunk
   store as an `xarray.Dataset` via `intake.open_virtual_icechunk()`.

## Builder API shape (for now)

Both builders are still intentionally **catalog-first**: the public entrypoint is a
pre-built intake-esm datastore.

That is slightly conservative, but deliberate. The two builder paths are not yet
symmetrical enough to justify a shared alternate-source API:

- the **virtual** builder fundamentally needs source asset paths plus parser / provenance
  context
- the **real-data** builder is a more plausible future home for dataset-driven helper
  APIs because it writes real chunks into Icechunk

There is an internal `GroupEntry` seam to support later experimentation, but this package
does **not** currently promise a shared public `from_dataset_dict(...)` /
`from_group_iterator(...)` surface across both builders.

## Installation

```bash
pip install intake-virtual-icechunk
```

## License

Apache-2.0. See `LICENSE` for details.
