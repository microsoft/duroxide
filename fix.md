# PR 22 Fix Triage

| No. | Priority | Status | Area | Improvement | Rationale |
| --- | --- | --- | --- | --- | --- |
| 1 | P0 | Done | SQLite migrations | Remove copyright header edits from existing `migrations/*.sql` files. | SQLx hashes the full migration SQL text, so comment-only edits to already-applied migrations can cause checksum mismatches for existing SQLite databases. |
| 2 | P1 | Done | README accuracy | Remove the README note that says providers persist history only and queues are in-memory runtime components. | Providers own durable queues as well as history; the added note contradicts the provider contract and SQLite schema. |
| 3 | P1 | Accepted | Contributing guidance | Keep both the Microsoft OSS text and the original pre-submit checklist in `CONTRIBUTING.md`. | The Microsoft OSS text is useful, but the rewrite weakened or displaced project-specific checklist guidance contributors should still see. |
| 4 | P1 | Accepted | Test guidance | Restore the original expectation to test happy path plus 1-2 edge cases. | The current wording only says to update tests for behavior changes, losing useful quality guidance from the original file. |
| 5 | P1 | Accepted | PR template | Update `.github/pull_request_template.md` to use `cargo nt` instead of `cargo test`. | The repo instructions require nextest; the template still points contributors at the older command. |
| 6 | P2 | Done | README scope | Remove the newly added `### Notes` block from the Duroxide family section. | The import, timer, unknown-instance, and replay-safe logging notes are technical guidance unrelated to copyright, code of conduct, security, CLA, or OSS policy. |
| 7 | P2 | Done | README scope | Do not keep the `duroxide runtime` to `Duroxide runtime` capitalization changes. | These are cosmetic branding edits rather than OSS compliance changes and are outside the stated PR purpose. |
| 8 | P2 | Dropped | Header coverage | Do not add copyright notices to config/build files: `.cargo/config.toml`, `.github/workflows/CI.yml`, `Cargo.toml`, `profiling/Dockerfile`, `profiling/docker-compose.yml`, `rust-toolchain.toml`, `rustfmt.toml`, and `sqlite-stress/Cargo.toml`. | These files do not need copyright headers for this PR. |
| 9 | P3 | Accepted | Formatting | Add a trailing newline to `CODE_OF_CONDUCT.md`. | The file currently has no final newline; low-risk cleanup while touching OSS metadata. |
