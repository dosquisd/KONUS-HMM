import enum

import polars as pl


class TransitionMatrix(enum.Enum):
    BEFORE = "before"
    AFTER = "after"
    ALL = "all"

    @staticmethod
    def __transition_matrix(df: pl.DataFrame) -> pl.DataFrame:
        transitions = (
            df.select("primordial")
            .with_columns(
                pl.col("primordial").alias("from_state"),
                pl.col("primordial").shift(-1).alias("to_state"),
            )
            .drop_nulls()
        )

        # Count transitions
        counts = transitions.group_by(["from_state", "to_state"]).agg(
            pl.len().alias("count")
        )

        # Ensure all G1/G2/G3 pairs exist (even if 0)
        states = ["G1", "G2", "G3"]
        all_pairs = pl.DataFrame(
            {
                "from_state": [s for s in states for _ in states],
                "to_state": [s for _ in states for s in states],
            }
        )

        counts_full = all_pairs.join(
            counts, on=["from_state", "to_state"], how="left"
        ).fill_null(0)

        # Calculate probabilities (each row sums to 1)
        row_totals = counts_full.group_by("from_state").agg(
            pl.col("count").sum().alias("total")
        )

        prob_matrix = (
            counts_full.join(row_totals, on="from_state")
            .with_columns((pl.col("count") / pl.col("total")).alias("prob"))
            .select(["from_state", "to_state", "prob"])
            .pivot(on="to_state", index="from_state", values="prob")
            .sort("from_state")
            .select(["from_state"] + states)  # Order columns as G1, G2, G3
        )

        return prob_matrix

    def transition_matrix(self, df: pl.DataFrame) -> pl.DataFrame:
        if self == TransitionMatrix.BEFORE:
            df = df.filter(pl.col("t2") <= 0)
            return self.__transition_matrix(df)
        elif self == TransitionMatrix.AFTER:
            df = df.filter(pl.col("t2") > 0)
            return self.__transition_matrix(df)
        elif self == TransitionMatrix.ALL:
            return self.__transition_matrix(df)
        else:
            raise ValueError(f"Unknown transition matrix: {self}")
