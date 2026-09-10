---
name: release-preparation
description: Prepare and open a Duroxide release pull request, then create its version tag after merge and explicit approval. Use when asked to prepare a release, bump the crate version, draft release notes, validate a release candidate, or complete the public release handoff.
---

# Duroxide Release Preparation

This source-owned skill prepares a release pull request and, after it is merged
and separately approved, creates the version tag that hands the release to
Microsoft-managed internal pipelines. Follow
[RELEASE_POLICY.md](../../../RELEASE_POLICY.md).

## Boundaries

Never:

- Add or modify a package-publishing workflow.
- Run `cargo publish`.
- Create a GitHub Release.
- Add publishing credentials or document internal pipeline operations.
- Modify files under `docs/proposals/`.
- Merge the release pull request.
- Create, move, or push a release tag before the release pull request is merged
  into `main`.
- Create or push a release tag without explicit user approval given after the
  merge.

## Required Input

Obtain the target semantic version from the user. Do not choose a major, minor,
or patch bump without confirmation.

Obtain permission to commit, push the release branch, and create the pull
request if the user's request did not explicitly authorize those actions.

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
cargo clippy --all-targets --all-features -- -D warnings -A clippy::uninlined_format_args
./run-tests.sh
cargo test --doc --all-features
cargo package --allow-dirty
cargo package --list --allow-dirty
```

`./run-tests.sh` is required because it runs nextest both with and without
feature flags. Do not replace it with a normal `cargo test` run.

Inspect the package file list for generated files, local artifacts, secrets,
and other unintended content.

## Create the Release Pull Request

After validation:

1. Review the complete diff and ensure it contains only the intended release
   preparation changes.
2. Commit with the title `chore(release): duroxide X.Y.Z`.
3. Push the release branch.
4. Create a pull request targeting `main` with the same title. Follow the
   repository pull request template and include the validation results.
5. Report the pull request URL and stop. Do not merge it or create the tag.

## Create the Post-Merge Tag

Run this phase only after the release pull request has merged and the user asks
to complete the release handoff.

1. Refresh the pull request state, `origin/main`, and remote tags.
2. Verify all of the following:
   - The release pull request is merged with `main` as its base.
   - Its merge commit is present on `origin/main`.
   - `Cargo.toml`, `CHANGELOG.md`, and `README.md` at that commit contain the
     target version.
   - The `vX.Y.Z` tag does not already exist locally or remotely.
3. Ask for explicit approval to create and push `vX.Y.Z`, naming the exact tag
   and merge commit SHA in the question. Approval given before the pull request
   merged does not satisfy this requirement.
4. After approval, create a lightweight tag, matching existing Duroxide release
   tags:

   ```bash
   git tag vX.Y.Z <merge-commit-sha>
   git push origin refs/tags/vX.Y.Z
   ```

5. Report the pushed tag and stop. Do not publish the crate or create a GitHub
   Release; the internal pipeline owns those operations.

## Completion Report

Report:

- The prepared version and release date.
- The files changed.
- The release-note summary.
- Validation failures, if any.
- The release pull request URL after preparation.
- The tag and tagged commit after the separately approved post-merge handoff.
