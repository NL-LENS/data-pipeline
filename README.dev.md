# Development documentation

TODO: finish

## Install dependencies

```bash
uv sync --extra dev

source .venv/bin/activate
uv pip install -e ./
pytest
```


## Running CI locally with `act`

Assuming `act` is available and on the PATH:

```bash
act -P ubuntu-latest=ghcr.io/catthehacker/ubuntu:act-latest -W .github/workflows/$WORKFLOW_FILE.yml > .ci_log.txt
cat .ci_log.txt | grep Job # see status of each job run
rm -rf .ci_log.txt
```



### Package version number

- We use [semantic versioning].
- The package version is stored in [`pyproject.toml`] (`project.version` and
  `tool.bumpversion.current_version`)
- To increase the version number in all of these places at once, use bump-my-version (e.g., uv run bump-my-version bump patch).
- Don't forget to update the version number before making a release!


## References

[semantic versioning]: https://semver.org/
[bump-my-version]: https://callowayproject.github.io/bump-my-version/
[`pyproject.toml`]: ./pyproject.toml
