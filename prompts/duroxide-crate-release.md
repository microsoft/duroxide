# Duroxide Crate Release Checklist

Use this prompt when preparing a crates.io release. Run it end-to-end and paste results in PR/issue as needed.

## Preconditions
- Workspace clean: `git status` empty (aside from intentional release files)
- All tests pass: `cargo test`, `cargo nextest run`, `cargo test --doc`
- Tooling available: `cargo-release` (if used), `just` (if used)

## Release Prep Steps
> Ask the author whether the tests below were already run before re-running anything.

1) **Sync main**
   - `git fetch origin && git checkout main && git pull`
   - Rebase your release branch onto main.

2) **Version bump**
   - Update `Cargo.toml` version.
   - Update `CHANGELOG.md` with date and highlights.
   - If workspace members exist, sync versions across crates.

   2a) **Link implemented proposals**
       - Check if this release implements any proposal from `docs/proposals/` or `docs/proposals-impl/`
       - If yes, add a **Proposal:** line after the release link in the changelog entry:
         ```
         **Proposal:** [Proposal Title](https://github.com/microsoft/duroxide/blob/main/docs/proposals-impl/proposal-name.md)
         ```
       - Multiple proposals can be listed on separate lines
       - This helps users understand the design rationale behind major changes

3) **Dependency audit**
   - `cargo update -p <dep>` for targeted bumps if needed.
   - `cargo deny check` (if configured) or `cargo audit`.

4) **Build & test matrix**
   - `cargo fmt --all`
   - `cargo clippy --all-targets --all-features -- -D warnings`
   - `cargo test`
   - `cargo nt` (nextest + provider validation tests)
   - `cargo test --doc`
   - Optional: `cargo test --features provider-test`

5) **Update README.md** ⚠️ CRITICAL - This is what crates.io displays!
   - Update the "Latest Release" line near the top:
     ```
     > **[Latest Release: vX.Y.Z](https://crates.io/crates/duroxide/X.Y.Z)** — Brief description of key changes.
     > See [CHANGELOG.md](CHANGELOG.md#0XYZ---YYYY-MM-DD) for release notes.
     ```
   - Match the description to CHANGELOG highlights
   - Update the anchor link format: `#0XYZ---YYYY-MM-DD` (version with dots removed, dashes between date parts)
   - If a major proposal was implemented, add a link: `[Proposal](docs/proposals-impl/proposal-name.md)`
   - **Verify the link works locally before committing**

6) **Artifacts**
   - `cargo package --locked` (verifies package can be built)
   - Inspect `cargo package --list` for unwanted files.

7) **Tag for release**
   - Tag: `git tag vX.Y.Z && git push origin vX.Y.Z`
   - ⚠️ **IMPORTANT**: Do NOT run `cargo publish --locked` yourself.
   - Microsoft's internal OSS release pipeline will automatically detect the tag and handle publishing to crates.io.
   - See [RELEASE_POLICY.md](../RELEASE_POLICY.md) for how the publishing infrastructure works.

8) **Post-release**
   - Monitor for the automatic GitHub Release creation (should appear within ~30 minutes).
   - If the release pipeline does not complete, contact the duroxide team.
   - No manual announcement needed; GitHub Release is created automatically.

## Release Validation Prompt (for Copilot / reviewers)
Use this condensed prompt to validate a release PR:

- Verify version bump in `Cargo.toml` and `CHANGELOG.md` date/notes.
- Ensure tests/clippy/fmt/docs were run (check CI or commands). Missing? Ask for runs.
- Confirm `cargo package --locked` passes and package contents exclude junk (target/, tmp, tests assets unless needed).
- Check for workspace member version alignment (if any).
- Ensure tag instruction matches version (vX.Y.Z).
- Confirm no API-breaking changes are undocumented.

## Notes
- Keep release PRs small: version bump + changelog + mechanical updates only.
- For hotfixes, branch from the last tagged release.
- If using `cargo-release`, align steps with its config (pre-release hooks, tagging).
