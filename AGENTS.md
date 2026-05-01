# AGENTS.md - VerFSNext

## Architecture
VerFSNext is a Rust COW FUSE filesystem storing files as UltraCDC chunks (XXH3-128 hashed) in zstd-seekable packs, with a KV-only metadata layer. Chunks are deduplicated and compressed in-line, and persisted to packs.Space is reclaimed by a GC when the system is idle.
### Features
    - Deduplication - in-line
    - Compression - in-line
    - Snapshots - Snapshots can be seem and interacted through the .snapshots folder at root
    - Encrypted .vault folder at root

# IMPORTANT NOTICE ABOUT COMPATIBILITY
We are now in production and any change that breaks compatibility with previous versions must provide a one time migration path.
This migration path must be documented in the migration guide and must be run automatically when the system starts, its code must be as separate as possible from the main code, compartamentalized and easy to remove after we discontinue support for the previous version.

## Main Priorities (in order of importance)
1. Never lose data
2. Keep and Enhance POSIX/rsync Operational Correctness
3. Never stall reads
4. Keep performance as high as possible
**Every decision must be made keeping these priorities in mind.**

## Commands
- Build: `cargo build --release`
- Run benchmark: `VERFSNEXT_RUN_MOUNT_TESTS=1 cargo test bench_comfyui_profile --test rsync_integration -- --nocapture`
  - Only on Linux with FUSE; requires `mountpoint`, `fusermount`, `bash`, `dd`, `sync`, `sha256sum`, `python3`

## Testing Policy
- Prefer writing integration tests over unit tests and always use OS commands when dealing with the filesystem, never write tests that interact with the internal API of the system.
- Run full tests on big changes.
- Run focused tests on small changes.
- Don't write tests for what the type system already guarantees.

### Code Organization
- **VENDOR**: code under `vendor/` has been hard forked and you must treat as our own code, making any needed fixes and modifications.

## Reference Documentation (create if it does not exist)
- [VerFS Technical Deep Dive](docs/technical_deep_dive.md)

## Always update documentation after big changes (create if it does not exist)
- [VerFS Technical Deep Dive](docs/technical_deep_dive.md)

## Update SurrealKV Architecture Documentation after any change to SurrealKV code
- [SurrealKV Architecture](vendor/surrealkv/docs/ARCHITECTURE.md)

## When working on issues/bugs, consult our Bug Fix history to better undertand impact and possible regressions
- [Bug fix history](docs/bug-fix-history.md)
- Always update this document after solving a bug or an issue

DO NOT CREATE NEW TESTS IF YOU ARE NOT EXPLICITLY ASKED TO DO SO.
DO NOT COMMENT OUT OR KEEP LEGACY CODE IN THE CODEBASE, IF YOU NEED TO CHANGE SOMETHING, CHANGE IT AND REMOVE THE OLD CODE.
DO NOT USE PIP, ALWAYS USE UV AND GIVE INSTRUCTIONS ON HOW TO USE IT.
DO NOT ADD FALLBACKS
DO NOT WRITE DEFENSIVE CODE
DO NOT SWALLOW ERRORS, ALWAYS MAKE THEM EXPLICIT AND LOG THEM
