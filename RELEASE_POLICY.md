# Duroxide Release Policy

Official Duroxide packages are published through Microsoft-managed internal
release pipelines. This public repository contains source code and validation
automation, plus the public release handoff described below. It intentionally
does not contain package-publishing workflows, publishing credentials, or
internal pipeline instructions.

## Public Release Handoff

Version 0.1.31 is reserved for the execution-pinned race-cancellation cutover.
Never tag or publish 0.1.31 or a later version from a branch that lacks the
queue, positional-wait, and exact continue-as-new carry-forward fixes.
The initial execution stamp selects these semantics permanently. An old
hotfix branch numbered 0.1.31+ would record decisions that corrected runtimes
cannot replay with that stamp. The subsequent release preparation uses 0.1.32;
the semantic threshold remains 0.1.31.

GitHub Actions in this repository are limited to building and testing changes.

1. Prepare a release pull request that updates the package version, changelog,
   and README release notice.
2. Merge the pull request into `main` through the normal review process.
3. After the merge, obtain the user's explicit approval to create the exact
   `vX.Y.Z` tag on the merged release commit.
4. Push the new tag. The tag is the handoff to the Microsoft-managed internal
   publishing pipeline.

Do not:

- Add or restore a GitHub Actions workflow that publishes packages or creates
  official releases.
- Publish an official package directly with `cargo publish` or another registry
  command.
- Create, move, or push a release tag before its pull request is merged into
  `main` or without explicit user approval.
- Create a GitHub Release manually.
- Add publishing credentials or tokens to this repository.
- Document or automate internal publishing operations in this repository.

## Internal Publishing

After the approved version tag is pushed, authorized Microsoft maintainers
monitor the release through the internal pipelines. Pipeline configuration,
credentials, approvals, and compliance controls are managed outside this
repository.

Published package versions and [CHANGELOG.md](CHANGELOG.md) are the public
record of available releases. Internal pipeline implementation details are not
required to build, test, or contribute to Duroxide.
