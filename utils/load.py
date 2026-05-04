import io
import re
from itertools import product
from pathlib import Path
from typing import Dict, List, Optional

import networkx as nx
import polars as pl

from .constants import DATADIR, MIN_VALUE_THRESHOLD
from .dtypes import EventData
from .hmm import TransitionMatrix
from .normalizer import CustomScale, Normalizer

EventID = str


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


def load_events(*, custom_scale_args: Optional[Dict[EventID, CustomScale]] = None) -> List[EventData]:
    if custom_scale_args is None:
        custom_scale_args = {}

    events: List[EventData] = []

    for year_event in list(filter(lambda x: x.is_dir(), DATADIR.glob("*"))):
        print(f"[+] Processing year: {year_event.name}")
        year_metadata = year_event / "event_metadata.csv"
        metadata = pl.read_csv(year_metadata)
        for event in year_event.glob("*.txt"):
            event_id = event.stem
            event_class = str(
                metadata.filter(pl.col("#") == int(event_id))["class"].item()[0]
            )
            event_intensity = float(metadata.filter(pl.col("#") == int(event_id))["class"].item()[1:])
            print(f"  [+] Processing event: {event_id} -- class: {event_class} -- intensity: {event_intensity}")

            df = load_data_per_event(event).drop_nulls().drop_nans()
            primordial = []
            for i, row in enumerate(df.iter_rows(named=True)):
                try:
                    if (
                        row["G3bgsub"] >= row["G2bgsub"]
                        and row["G3bgsub"] >= row["G1bgsub"]
                    ):
                        primordial.append("G3")
                    elif (
                        row["G2bgsub"] >= row["G1bgsub"]
                        and row["G2bgsub"] >= row["G3bgsub"]
                    ):
                        primordial.append("G2")
                    elif (
                        row["G1bgsub"] >= row["G3bgsub"]
                        and row["G1bgsub"] >= row["G2bgsub"]
                    ):
                        primordial.append("G1")
                    else:
                        print(
                            f"  [!] Skipping row with NaN values ({i}) for event {event_id}: {row}"
                        )
                except TypeError:
                    print(
                        f"  [!] Skipping row with NaN values ({i}) for event {event_id}: {row}"
                    )

            try:
                df = df.with_columns(pl.Series("primordial", primordial))
            except Exception:
                print(f"  [!] Failed to add 'primordial' column for event {event_id}")
                raise

            combinations = product(TransitionMatrix, Normalizer)
            for tm, norm in combinations:
                print(f"    [?] Transition Matrix: {tm.value} -- Normalizer: {norm.value}")

                # Normalize the data using the specified normalizer
                normalized_data = norm.normalize(
                    df.select(pl.col("G1bgsub", "G2bgsub", "G3bgsub")).to_numpy(),
                    scale=custom_scale_args.get(event_id, 1) if norm == Normalizer.CUSTOM else None,
                )
                normalized_df = (
                    pl.DataFrame(
                        normalized_data, schema=["G1bgsub", "G2bgsub", "G3bgsub"]
                    )
                    .with_columns(pl.Series("primordial", df["primordial"]))
                    .with_columns(pl.Series("t1", df["t1"]))
                    .with_columns(pl.Series("t2", df["t2"]))
                )

                prob_matrix = (
                    (
                        tm.transition_matrix(normalized_df)
                        .fill_null(0.0)
                        .fill_nan(0.0)
                        .sort("from_state")
                        .select(["from_state"] + ["G1", "G2", "G3"])
                    )
                    .to_pandas()
                    .replace({0: MIN_VALUE_THRESHOLD})
                )

                graph = nx.from_pandas_adjacency(
                    prob_matrix.set_index("from_state"), create_using=nx.DiGraph
                )
                event_data = EventData(
                    event_class=event_class,
                    event_intensity=event_intensity,
                    event_tm=graph,
                    tm_thecnique=str(tm.value),
                    normalizer=str(norm.value),
                )
                events.append(event_data)

    return events
