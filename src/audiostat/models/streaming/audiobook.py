import polars as pl
from audiostat.models.streaming.extended import ExtendedStreamingHistory


class AudiobookStreamingHistory:
    """Generic class representing extended audiobook streaming history."""

    schema: dict = {
        "ts": pl.Datetime,
        "ms_played": pl.Int64,
        "audiobook_title": pl.String,
        "audiobook_uri": pl.String,
        "audiobook_chapter_uri": pl.String,
        "audiobook_chapter_title": pl.String,
        "reason_start": pl.Categorical,
        "reason_end": pl.Categorical,
        "shuffle": pl.Boolean,
        "skipped": pl.Boolean,
        "offline": pl.Boolean,
        "offline_timestamp": pl.Int32,
        "incognito_mode": pl.Boolean,
    }

    @classmethod
    def loader(cls, *source: str, data: pl.DataFrame | None = None) -> pl.DataFrame:
        if data is None:
            data = ExtendedStreamingHistory.loader(*source)
        return data.select(*cls.schema.keys()).filter(
            pl.col("audiobook_uri").is_not_null()
        )
