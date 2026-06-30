# Contributing

Thanks for your interest in improving duroxide!

This project welcomes contributions and suggestions. Most contributions require you to
agree to a Contributor License Agreement (CLA) declaring that you have the right to,
and actually do, grant us the rights to use your contribution. For details, visit
https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need
to provide a CLA and decorate the PR appropriately (for example, label or comment).
Simply follow the instructions provided by the bot. You will only need to do this once
across all repositories using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Reporting security issues

Please do not report security vulnerabilities through public GitHub issues. Follow the instructions in [SECURITY.md](SECURITY.md).

## Before submitting changes

- Code
  - [ ] Regular tests pass locally (`cargo nt`)
  - [ ] Lints/clippy (if applicable) are clean
  - [ ] Tests added or updated for behavior changes
- Documentation
  - [ ] Does this change affect existing docs? Update them.
  - [ ] Is this a new surface area or concept? Add a doc under `docs/`.
  - [ ] Link new docs from `docs/README.md`.
- Design notes (optional but encouraged)
  - [ ] For non-trivial changes, include a short design rationale in the PR description with code pointers.

## Development workflow

- Write or update tests first (happy path + 1-2 edge cases).
- Keep public APIs stable where possible; note breakages clearly.
- Prefer small, focused commits with descriptive messages.
- For non-trivial changes, include a short design rationale in the pull request description with code pointers.
- Update documentation when a change affects existing docs, introduces a new surface area, or changes behavior users rely on.

Before opening a pull request, run the checks relevant to your change:

```bash
cargo nt
cargo clippy --all-targets --all-features
cargo test --doc --all-features
```

For broader runtime changes, also run the comprehensive two-pass suite:

```bash
./run-tests.sh
```

## Filing pull requests

Use the pull request template in [.github/pull_request_template.md](.github/pull_request_template.md) and fill in the docs checkboxes.
