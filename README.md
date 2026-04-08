# intake-virtual-icechunk

> [!WARNING]
> This package has been scaffolded by Claude but none of the implementation is complete yet.
> APIs, class names, and behaviour are all subject to change.

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

1. **Building** (`IcechunkStoreBuilder`) — given a pre-built intake-esm catalog, creates
   virtual references with VirtualiZarr and writes each dataset as a named Zarr group
   inside a single Icechunk store.
2. **Reading** (`IcechunkSource`) — an intake driver for opening a group from an Icechunk
   store as an `xarray.Dataset` via `intake.open_virtual_icechunk()`.

## Installation

```bash
pip install intake-virtual-icechunk
```

## License

Apache-2.0. See `LICENSE` for details.
