# Duroxide Release Policy

Official Duroxide packages are published through Microsoft-managed internal
release pipelines. This public repository contains source code and validation
automation, but intentionally does not contain package-publishing workflows,
publishing credentials, or operational release instructions.

## Public Repository Responsibilities

GitHub Actions in this repository are limited to validation such as building,
testing, and linting changes. Contributors may update package metadata and the
changelog through normal pull requests when coordinated with maintainers.

Do not:

- Add or restore a GitHub Actions workflow that publishes packages or creates
  official releases.
- Publish an official package directly with `cargo publish` or another registry
  command.
- Add publishing credentials or tokens to this repository.
- Add release-operation skills or prompts that duplicate the internal process.

## Internal Publishing

Authorized Microsoft maintainers initiate and monitor releases through the
internal pipelines. Pipeline configuration, credentials, approvals, and
compliance controls are managed outside this repository.

Published package versions and [CHANGELOG.md](CHANGELOG.md) are the public
record of available releases. Internal pipeline implementation details are not
required to build, test, or contribute to Duroxide.
