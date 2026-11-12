# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Option to read `.sav` files with multiprocessing in `stream_to_bronze`, `BronzeConfig`
  and the CLI (`--num_processes`), via [PR #40](https://github.com/NL-LENS/data-pipeline/pull/40).
  The default is no multiprocessing.
- Option `n_rows` in `stream_to_bronze` to read only the first rows of a file.

### Removed

### Changed
- The default chunk size for reading `.sav` files is now 500,000 rows (was 100,000).
- `ingest_source` takes a `BronzeConfig` instead of `chunk_size`.


## [0.0.1] - 2026-11-11

### Added
- Added `BronzeConfig` class, via [PR #75](https://github.com/NL-LENS/data-pipeline/pull/75)
  - The class be used for passing parameters to `build_bronze`
  - The CLI arguments changed: from `source_regex` to `source_filename_regex`,
  and additional flag `source_path_regex` was added.
- Silver format changed, and `SilverConfig` fields have changed, via [PR #73](https://github.com/NL-LENS/data-pipeline/pull/73)
- Function to convert from bronze to silver (no CLI), via [PR #44](https://github.com/NL-LENS/data-pipeline/pull/44)
- CLI for `init` and `build bronze`, via [PR #26](https://github.com/NL-LENS/data-pipeline/pull/26).
- Feature for processing `.sav` files to parquet, via [PR #22](https://github.com/NL-LENS/data-pipeline/pull/22).
- Feature for collecting metadata, via [PR #13](https://github.com/NL-LENS/data-pipeline/pull/13).



[0.0.1]: https://github.com/NL-LENS/data-pipeline/releases/tag/v0.0.1
