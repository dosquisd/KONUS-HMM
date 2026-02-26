import io
import re
from pathlib import Path
from typing import Optional

import polars as pl


def load_data_per_event(
    file_path: Path | str,
    *,
    skip_rows: int = 11,
    nan_values: Optional[str] = "nan",
) -> pl.DataFrame:
    with open(file_path) as f:
        lines = f.readlines()[skip_rows:]

    # Normalize multiple spaces to a single comma
    cleaned = "\n".join(
        re.sub(r"\s+", ",", line.strip()) for line in lines if line.strip()
    )

    return pl.read_csv(io.StringIO(cleaned), null_values=nan_values)
    # Yes, with pandas it would be as simple as:
    # return pd.read_csv(file_path, skiprows=skip_rows, delimiter=r"\s+")
