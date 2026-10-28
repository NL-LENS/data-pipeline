# Development documentation

## Clone the repository

```shell
git clone https://github.com/NL-LENS/data-pipeline.git
```

Use a specific branch for each feature or bug fix, as follows:
```shell
git switch -c feature
```

## Dependencies

To get all dependencies required for development, use

```shell
uv sync --extra dev
```

You can add more dependencies with
```shell
uv add foo
```

You should check if a dependency is required for a user, and if not,
add it as a specific optional dependency, for instance for development only.

## Testing

We use [`pytest`] for testing. To run tests, do

```shell
uv run pytest
```

Or
```shell
source .venv/bin/activate
uv pip install -e ./
pytest
```

To add tests, consider the existing structure of tests and the documentation.

## Code quality

For consistent formatting and code quality, we use [`ruff`] and [`mypy`].

To check if your changes are in line with the repository's style, run
```shell
uv run ruff check
uv run ruff format
```

## Pre-commit checks

We use [`prek`](https://prek.j178.dev/) to run checks (including ruff)
before each commit. The checks are
configured in [`prek.toml`].

To enable them, install the Git hook once after cloning the repository:

```shell
uv run prek install
```

Whenever you make a commit to a branch of the repository, the checks are run and the commit is rejected if they fail.
Make sure that all checks pass before you commit. You can also run the checks on all files manually:

```shell
uv run prek run --all-files
```

To update the hooks to their latest versions, run:

```shell
uv run prek update
```

## Package version number

- We use [semantic versioning].
- The package version is stored in [`pyproject.toml`] (`project.version` and
  `tool.bumpversion.current_version`)
- To increase the version number in all of these places at once,
  use [bump-my-version] (e.g., `uv run bump-my-version bump patch`).
- Don't forget to update the version number before making a release!


## Running continuous integration locally with `act`

For debugging failures on continuous integration, one can use [`act`].

After installing it and adding it to your PATH, run:
```bash
act -P ubuntu-latest=ghcr.io/catthehacker/ubuntu:act-latest -W .github/workflows/my-workflow.yml > .ci_log.txt
cat .ci_log.txt | grep Job # see status of each job run
rm -rf .ci_log.txt
```

This works reliably only for tests that run on Linux/Ubuntu images.


## References

These guidelines were adapted from [popylar-org/prfmodel].

[semantic versioning]: https://semver.org/
[bump-my-version]: https://callowayproject.github.io/bump-my-version/
[`pyproject.toml`]: ./pyproject.toml
[`act`]: https://nektosact.com/
[`pytest`]: https://docs.pytest.org/en/stable/
[`prek.toml`]: ./prek.toml
[`ruff`]: https://docs.astral.sh/ruff/
[`mypy`]: https://mypy.readthedocs.io/en/stable/
[`prek`]: https://prek.j178.dev/
[popylar-org/prfmodel]: https://github.com/popylar-org/prfmodel/
