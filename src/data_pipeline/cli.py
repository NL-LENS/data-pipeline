"""CLI for data pipeline."""

import argparse
import importlib.metadata
import logging
from pathlib import Path
from data_pipeline.metadata import run_init

version_string = importlib.metadata.version("data_pipeline")


def cli_main() -> None:
    """CLI function for running data pipeline."""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    parser.add_argument("--version", action="version", version=f"%(prog)s {version_string}")

    init_parser = subparsers.add_parser("init")
    init_parser.add_argument("--root", type=Path, help="Data root directory.")
    init_parser.add_argument("--db_file", type=Path, help="Path to the database file to be created.")
    init_parser.add_argument(
        "-v", dest="verbose", action="count", default=0, help="Set verbosity: -v for INFO, -vv for DEBUG"
    )

    args = parser.parse_args()

    log_level = 10 * (2 - args.verbose)  # logging.INFO = 20, logging.DEBUG = 10
    logging.basicConfig(level=log_level)

    match args.command:
        case "init":
            run_init(args.root, args.db_file)


# Notes
# source_path should be relative to the db_file? or to what? should it be abs?
# add simple logging
