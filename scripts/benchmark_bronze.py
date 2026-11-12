"""Benchmark streaming a .sav file to bronze Parquet with different settings.

Each combination of chunk size and number of processes reads the same
leading fraction of the file. Output files are written to a temporary
directory and removed afterwards.

Example
-------
python scripts/benchmark_bronze.py /path/to/file.sav \
    --chunk_sizes 100000 500000 --num_processes 1 4 --fraction 0.1
"""

import argparse
import logging
import math
import os
import sys
import tempfile
from itertools import product
from pathlib import Path
from time import perf_counter
from data_pipeline.bronze import DEFAULT_CHUNKSIZE
from data_pipeline.bronze import read_sav_meta
from data_pipeline.bronze import stream_to_bronze

logger = logging.getLogger("benchmark_bronze")


def available_cpu_count() -> int:
    """Return the number of CPUs this process is allowed to use."""
    # `os.sched_getaffinity` respects CPU restrictions on the process,
    # but it is only available on Linux.
    if sys.platform == "linux":
        return len(os.sched_getaffinity(0))
    return os.cpu_count() or 1


def time_stream_to_bronze(
    source_file: Path,
    dest_file: Path,
    chunk_size: int,
    num_processes: int,
    n_rows: int,
) -> float:
    """Return the seconds `stream_to_bronze` takes for one setting."""
    start = perf_counter()
    stream_to_bronze(source_file, dest_file, chunk_size, num_processes=num_processes, n_rows=n_rows)
    elapsed = perf_counter() - start
    dest_file.unlink()
    return elapsed


def main() -> None:
    """Run the benchmark for all combinations of chunk size and number of processes."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("source_file", type=Path, help="The .sav file to read.")
    parser.add_argument(
        "--chunk_sizes", nargs="+", type=int, default=[DEFAULT_CHUNKSIZE], help="Chunk sizes to benchmark."
    )
    parser.add_argument("--num_processes", nargs="+", type=int, default=[1], help="Numbers of processes to benchmark.")
    parser.add_argument("--fraction", type=float, default=0.1, help="Fraction of the rows to read.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    total_rows = read_sav_meta(args.source_file).number_rows or 0
    n_rows = math.ceil(total_rows * args.fraction)
    logger.info("File: %s", args.source_file)
    logger.info("CPUs available: %d", available_cpu_count())
    logger.info("Rows read: %d of %d (%.0f%%)", n_rows, total_rows, 100 * args.fraction)

    with tempfile.TemporaryDirectory() as tmp_dir:
        dest_file = Path(tmp_dir) / "benchmark.parquet"
        for chunk_size, num_processes in product(args.chunk_sizes, args.num_processes):
            elapsed = time_stream_to_bronze(args.source_file, dest_file, chunk_size, num_processes, n_rows)
            throughput = n_rows / elapsed if elapsed > 0 else math.nan
            logger.info(
                "chunk_size=%d num_processes=%d: %.2fs (%.2f min), %.0f rows/s",
                chunk_size,
                num_processes,
                elapsed,
                elapsed / 60,
                throughput,
            )


# The guard is required for multiprocessing on Windows and macOS.
if __name__ == "__main__":
    main()
