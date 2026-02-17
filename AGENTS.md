# AGENTS.md - VerFS3

[YOU SHOULD NEVER, FOR ANY REASON ADD BACKWARD COMPATIBILITY TO ANYTHING IF YOU
WHERE NOT EXPLICITLY ASKED FOR. **ALL CHANGES ARE BREAKING** AND **WE DO NOT CARE ABOUT THOSE LEFT BEHIND**, THEY SHOULD NOT EVEN BE MENTIONED. THIS RULE IS SACRED AND UNBREAKABLE.]

## Main Priorities (in order of importance)
1. Never lose data
2. Keep and Enhance POSIX/rsync Operational Correctness
3. Never stall reads
4. Keep performance as high as possible
**Every decision must be made keeping these priorities in mind.**

## Commands
- Build: `cargo build --release`

## Testing Policy
- Run full tests on big changes
- Run focused tests on small changes

## Architecture
VerFSNext is a Rust COW FUSE filesystem storing files as UltraCDC chunks (XXH3-128 hashed) in zstd-seekable packs, with a KV-only metadata layer. Chunks are deduplicated and compressed in-line, and persisted to packs.Space is reclaimed by a GC when the system is idle.
### Features
    - Deduplication - in-line
    - Compression - in-line
    - Snapshots - Snapshots can be seem and interacted through the .snapshots folder at root
    - Encrypted .vault folder at root
### Code Organization
- **VENDOR**: code under `vendor/` has been hard forked and you must treat as our own code, making any needed fixes and modifications.

## Reference Documentation (create if it does not exist)
- [VerFS Technical Deep Dive](docs/technical_deep_dive.md)

## Always update documentation after big changes (create if it does not exist)
- [VerFS Technical Deep Dive](docs/technical_deep_dive.md)

## Update SurrealKV Architecture Documentation after any change to SurrealKV code
- [SurrealKV Architecture](vendor/surrealkv/docs/ARCHITECTURE.md)
