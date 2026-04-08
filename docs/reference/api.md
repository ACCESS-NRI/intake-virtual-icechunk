(api)=
# API reference

`intake_virtual_icechunk` provides two main components:

- `intake_virtual_icechunk.core.IcechunkSource` — an intake driver for opening a Zarr
  group from an Icechunk store as an `xarray.Dataset`. Registered as the
  `virtual_icechunk` driver, so `intake.open_virtual_icechunk()` is available automatically.
- `intake_virtual_icechunk._build.IcechunkStoreBuilder` — builds a virtual Icechunk store
  from a pre-built intake-esm catalog, writing one Zarr group per dataset using VirtualiZarr.

The following API summary is auto-generated.

```{autoclass} intake_virtual_icechunk.core.IcechunkSource
:members:
:noindex:
:special-members: __init__
```

```{autoclass} intake_virtual_icechunk._build.IcechunkStoreBuilder
:members:
:noindex:
:special-members: __init__
```
