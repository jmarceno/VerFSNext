# VerFSNext Migration Guide

VerFSNext is in production, so a new version must keep reading every data
directory written by an older one. Changes that alter on-disk metadata ship a
one-time migration instead of a read-path fallback. Migrations are
compartmentalized under `src/migration/` and must be easy to delete once the
previous format is out of support.

Every migration below runs automatically, in order, before a mount serves any
I/O. If a migration cannot finish, the mount fails with an explicit error; the
data stays as it was (each migration commits in transactions) and nothing is
served from a half-migrated directory.

## Running the migrations

- **Daemon:** `verfsnext mount` runs every pending migration in `VerFs::new`
  before opening packs and serving requests. No operator action is needed.
- **Offline commands:** `verfsnext snapshot`, `pack-size-migrate` and
  `gc offline` open the metadata store directly; they run the same pending
  migrations first, so offline commands never see a mixed layout.
- **One daemon only:** migrations run under the metadata store lock. Do not
  mount the same data directory twice.
- **Downgrade is not supported after a migration commits.** An older binary
  does not know the new layout. Keep a backup or a snapshot from before the
  upgrade if rollback may be needed. The migrated data stays forward-compatible
  with every later version.

Migrations keep their marker under a `SYS:` key. A migration whose marker is
present is a no-op. Removing a migration from the code after support for its
source format ends is safe: it only means such old directories are no longer
accepted.

## Inventory

| Marker (`SYS:`) | Runs when | Summary |
|---|---|---|
| `pack_index_crc32_migration_v1` | marker missing | Rewrites every `.idx` with the fixed-size record that carries `payload_crc32`. |
| `pack_max_size_mb` | key missing | Stores the configured pack size on first mount (legacy upgrade path). |
| `vault_chunk_keys_migration_v1` | marker missing | Moves vault chunk records (and inline payloads) from the shared chunk key to the domain-tagged vault key. |

## Vault chunk key migration (`vault_chunk_keys_migration_v1`)

### Why

The vault content hash is computed as `xxh3_128(0xA7 || plaintext)` while the
plain content hash is `xxh3_128(plaintext)`. The domain byte is a prefix to the
same hash stream, so the vault hash of `V` **equals** the plain hash of
`0xA7 || V`. Older versions stored every chunk record, inline payload, chunk
cache entry and reference count under the hash alone. A plain file whose content
started with `0xA7` could therefore alias a vault file's chunk: the second
writer deduplicated onto the first record, its bytes were never stored, and the
file read back with `EIO` (or was served the other domain's plaintext).

### What it does

Chunk identity is now the pair `(domain, hash)` (`crate::types::ChunkId`):

- plain records keep the historical key `C` + hash (no plain data is touched);
- vault records live at `C` + `0xA7` + hash;
- inline payloads follow the same rule (`T` + hash for plain, `T` + `0xA7` +
  hash for vault).

The migration scans chunk records in batches (transaction per batch), recognizes
vault records by `CHUNK_FLAG_ENCRYPTED`, and moves each record to the tagged
key, moving its inline payload in the same transaction. It never decrypts a
payload, so it runs while the vault is locked. Already-tagged records are
skipped, which makes a rerun after a crash a no-op for the moved records.

If the store never had a vault (`SYS:vault.wrap` absent), no vault record can
exist and the scan is skipped entirely.

### What it cannot fix

When the old alias was hit, one of the two files lost its bytes: the second
writer's data was never written to a pack. The migration classifies the
surviving record correctly, but the lost bytes are gone. After upgrading, run
`verfsnext check` (or open Overview → Health in the desktop app). It reports
the affected file under "missing pieces" with its path, so it can be restored
from a backup or moved to `lost+found`. Healthy files are unaffected.

### Validation

1. Update the binary and start the daemon normally. The log line
   `moved vault chunk records into the domain-tagged key space` appears once.
2. Unlock the vault and read a few vault files; they must read exactly as
   before.
3. Run `verfsnext check` and confirm no new refcount drift or missing pieces
   beyond files known to be damaged by the old defect.
4. Keep the pre-upgrade backup or snapshot until this validation passes.

### Rollback

A downgrade after the marker is written is not supported: the old binary looks
up the untagged key for vault chunks, so vault files would fail with `EIO`, and
its GC does not recognize the tagged keys. To roll back, restore the data
directory (or snapshot) taken before the upgrade, not just the binary.
