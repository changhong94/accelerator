# Immutable presence index sharing

`KUASAR_IMMUTABLE_INDEX_CACHE` opts host processes into sharing validated template
presence indexes. Use a dedicated directory owned by the process UID, mode 0700,
on tmpfs, for example `/run/fork2/immutable-index-cache`. Configuration is captured
at process initialization, before guest task environments are applied.

The original template is still read and validated on every open. Only the resulting
immutable index is deduplicated by SHA-256 and mapped read-only/shared. An existing
cache file is compared with the authoritative bytes before use. Mutable upper-disk
bitmaps are never put here. Missing, unsafe, full, or corrupt cache entries fall back
to validated heap data; this cache is not part of snapshot persistence.

Each process retains at most 8 MiB of mappings until exit. The directory keeps at
most 64 MiB and 128 published files. Eviction unlinks files without truncating them;
existing mappings remain valid, so mapped-but-unlinked pages can temporarily exceed
the directory budget. Files are mode 0400. Nothing is written to persistent storage.
All source authentication, disk sync, and checkpoint barriers remain in their
original paths.

For deployment validation, inspect `/proc/PID/smaps`: prepared runners should map
identical `.idx` device/inode pairs as `r--s`. Compare aggregate PSS rather than RSS,
since RSS counts the same shared pages once per process.
