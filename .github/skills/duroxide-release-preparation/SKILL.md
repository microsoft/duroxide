---
name: duroxide-release-preparation
description: Prepare Duroxide release metadata and validate the crate without publishing it. Use when asked to prepare a release, bump the crate version, draft release notes, or validate a release candidate.
license: Apache-2.0
metadata:
  author: duroxide
  version: "1.0"
---

# Duroxide Release Preparation

Prepare a release candidate in the public repository. Follow
[RELEASE_POLICY.md](../../../RELEASE_POLICY.md): official publishing is handled
by Microsoft-managed internal pipelines.

## Boundaries

This skill prepares repository changes only.

Never:

- Add or modify a package-publishing workflow.
- Run `cargo publish`.
- Create or push a release tag.
- Create a GitHub Release.
- Add publishing credentials or document internal pipeline operations.
- Modify files under `docs/proposals/`.
- Commit or push unless the user explicitly requests it.

## Required Input

Obtain the target semantic version from the user. Do not choose a major, minor,
or patch bump without confirmation.

Use the current date in `YYYY-MM-DD` format unless the user provides a release
date.

## Preparation Steps

1. Read `RELEASE_POLICY.md`, `Cargo.toml`, the top of `CHANGELOG.md`, and the
   release notice near the top of `README.md`.
2. Inspect `git status`, the latest `v*` tag, and changes since that tag.
   Preserve unrelated working-tree changes.
3. Confirm the target version is valid semver and greater than the current
   `Cargo.toml` package version.
4. Update the root `Cargo.toml` package version. Do not change the independent
   `sqlite-stress` package version.
5. Keep `## [Unreleased]` at the top of `CHANGELOG.md` and add the release
   section immediately below it:

   ```markdown
   ## [X.Y.Z] - YYYY-MM-DD

   **Release:** <https://crates.io/crates/duroxide/X.Y.Z>
   ```

6. Move prepared entries from `Unreleased` into the new section. Use the
   existing Keep a Changelog categories and derive notes only from verified
   repository changes. Preserve relevant pull request references and document
   breaking changes or migration requirements clearly.
7. Update the `README.md` latest-release notice with:
   - The target crates.io version URL.
   - A concise summary grounded in the changelog.
   - The changelog anchor used by the existing README format, for example
     `#0131---2026-09-07` for version `0.1.31`.
8. Review the diff and ensure release preparation contains only intentional
   metadata, release notes, and any changes the user explicitly requested.

## Validation

Run the repository's existing checks:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features
./run-tests.sh
cargo test --doc --all-features
cargo package --allow-dirty
cargo package --list --allow-dirty
```

`./run-tests.sh` is required because it runs nextest both with and without
feature flags. Do not replace it with a normal `cargo test` run.

Inspect the package file list for generated files, local artifacts, secrets,
and other unintended content.

## Completion

Report:

- The prepared version and release date.
- The files changed.
- The release-note summary.
- Validation failures, if any.

Leave the changes local and stop. Publishing and all official release
operations are performed through the internal pipeline.
