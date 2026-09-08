"""CLI for data pipeline."""

import argparse
import importlib.metadata
import logging
from pathlib import Path
from data_pipeline.bronze import build as build_bronze
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

    build_parser = subparsers.add_parser("build")
    build_parser.add_argument("color", choices=["bronze"], type=str, help="Which color layer to build")
    build_parser.add_argument("--db_file", type=Path, help="Path to the database file with metadata.")
    build_parser.add_argument("--source_regex", type=str, help="Filters source_path on matching strings")
    build_parser.add_argument(
        "--ref_period", nargs=2, help="Start and end year (both included) of reference to process."
    )
    build_parser.add_argument("--dest_dir", type=Path, help="Path to the processed bronze data.")
    # TODO: how do I pass in list to argparse?

    all_subparsers = [init_parser, build_parser]
    for p in all_subparsers:
        p.add_argument(
            "-v", dest="verbose", action="count", default=0, help="Set verbosity: -v for INFO, -vv for DEBUG"
        )

    args = parser.parse_args()

    log_level = 10 * (2 - args.verbose)  # logging.INFO = 20, logging.DEBUG = 10
    # TODO: what is the standard for logging? is logging used to send
    # "normal" messages to the user, or a different library?
    logging.basicConfig(level=log_level)

    match args.command:
        case "init":
            run_init(args.root, args.db_file)
        case "build":
            if args.color == "bronze":
                start_year = int(min(args.ref_period))
                end_year = int(max(args.ref_period))
                build_bronze(args.db_file, args.source_regex, start_year, end_year, args.dest_dir)
            else:
                raise NotImplementedError


# TODO: see security of datapackage spec -> no absolute file paths??

# Notes
# source_path should be relative to the db_file? or to what? should it be abs?
# add simple logging

# how to design an ergonomic way to give options for files?
# - pattern-match on source path, ie "SPOLISBUS"?
# - filter on ref_period -> years with nulls are ignored
# - versions: only use latest
# - check for read access!
# - perhaps check also Javier's prototype again?
