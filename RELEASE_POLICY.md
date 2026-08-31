# Duroxide Release Policy

## Overview

Duroxide is published to [crates.io](https://crates.io/crates/duroxide) by Microsoft's internal release infrastructure. This document outlines the public release workflow and internal publishing process.

## For Open Source Contributors

### Publishing a Release (Contributor Workflow)

**For external contributors:** Submit a PR that updates the version in `Cargo.toml` and adds a corresponding entry to `CHANGELOG.md`. Microsoft maintainers review and merge the version bump.

**For Microsoft maintainers:** After merging a version bump PR to `main`:

1. Create a **release PR** with:
   - Updated `Cargo.toml` version
   - Updated `CHANGELOG.md` (with date and highlights)
   - Updated `README.md` "Latest Release" section
   - Verification that all tests pass: `cargo test`, `cargo clippy --all-targets --all-features`, `cargo fmt`
   - Output of `cargo package --locked` (to verify the package is buildable)

2. After merge to `main`, create a **git tag**:
   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

3. Microsoft's internal OSS Release pipeline automatically:
   - Detects the new tag
   - Validates the crate integrity and authenticity
   - Publishes to crates.io via a signed, audited internal release process
   - Creates a GitHub Release with notes linking to the CHANGELOG

**Full release workflow instructions** are in [prompts/duroxide-crate-release.md](prompts/duroxide-crate-release.md).

### What Contributors Should NOT Do

- Do NOT run `cargo publish --locked` yourself
- Do NOT create GitHub Releases manually (the release pipeline handles this)
- Do NOT assume crates.io credentials are needed for contributors

## Publishing Infrastructure (Microsoft Internal)

Microsoft maintains an internal OneBranch pipeline that:

- Monitors duroxide git tags matching `v*`
- Validates the build artifact matches the public source commit
- Runs compliance checks (SBOM, security scanning)
- Publishes to crates.io using Microsoft's signed release credentials
- Creates audit logs and release metadata for compliance

This separation ensures:

1. **Authenticity**: Crates are published by Microsoft's infrastructure, with clear audit trails
2. **Consistency**: Every release follows the same validated process
3. **Security**: Publishing credentials are not distributed to contributors
4. **Compliance**: Internal publishing includes SBOM, SPDX metadata, and security scanning

## Crate Signing & Integrity

**Note:** Rust/crates.io does **not** support cryptographic signatures on published crates. Trust is established via:

- **HTTPS transport security** between your machine and crates.io
- **crates.io account security** (API token access control)
- **Cargo.lock** checksums (for reproducible builds in projects that pin crate versions)

The duroxide crate is delivered over HTTPS from crates.io's CDN. Verify the checksum in your `Cargo.lock` matches the expected value if you need bit-for-bit verification.

## Release Schedule & Support

- **Preview Phase**: duroxide is currently in preview (`v0.1.x`)
- **Semantic Versioning**: We follow semver; breaking changes will increment the minor version until `v1.0.0`
- **Supported Versions**: Currently only the latest release on crates.io is actively maintained

See [CHANGELOG.md](CHANGELOG.md) for release history and [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines.

## Questions?

If you have questions about releases, publishing, or the contribution process, please open an issue on GitHub.
