from pathlib import Path
import numpy as np
import pytest


class TestData:
    """Base class for creating test data frames."""

    sample_size: int = 100_000
    validity_mask: np.ndarray = np.ones(100_000)
    rng: np.random._generator.Generator = np.random.default_rng(1234)

    def make_identifiers(self, max_id: int = 1_000_000) -> np.ndarray:
        """Create an array of identifiers in string format."""
        ids = list(self.rng.integers(1, max_id, size=self.sample_size))
        ids = [f"{x:09d}" for x in ids]
        return np.array(ids)

    def make_categorical(self, categories: list[str], probs: list[float] | None = None) -> np.ndarray:
        """Make a categorical array."""
        return self.rng.choice(categories, p=probs, size=self.sample_size)

    def make_continuous(self) -> np.ndarray:
        """Make a continuous array."""
        return self.rng.random(size=self.sample_size).astype(np.float64)

    def corrupt(
        self, x: np.ndarray, inject: list, size: int, valid: bool = True, probs: list[float] | None = None
    ) -> np.ndarray:
        """Corrupt an array with a value.

        Arguments
        ---------
        x:
            array to modify
        inject:
            List of values to inject, chosen randomly according to `probs`.
        size:
            Number of rows to corrupt. Should be no larger than `x`.
        valid:
            If True, the rows with the corrupted data are expected
            to be modified in silver.
            If False, the rows with the corrupted data
            are expected to be dropped in silver.
        probs:
            If given, probabilities for the respective elements in `inject`.
            Defaults to equal probabilities.
        """
        missing_idx = self.rng.choice(np.arange(self.sample_size), size, replace=False)
        x[missing_idx] = self.rng.choice(inject, size, p=probs)
        if not valid:
            self.validity_mask[missing_idx] = 0
        return x

    @pytest.fixture
    def db_file(self, tmp_path: Path) -> Path:
        """File for metadata database."""
        return tmp_path / "metadata.db"

    def random_dates(self) -> np.ndarray:
        """Generate a set of random dates."""
        start_date = np.datetime64("2000-02-13")
        end_date = np.datetime64("2025-12-25")
        day_range = (end_date - start_date).item().days

        dates = np.repeat(start_date, self.sample_size)
        int_deltas = self.rng.integers(1, day_range, self.sample_size)
        time_deltas = np.array(int_deltas, dtype=np.timedelta64)

        return dates + time_deltas
