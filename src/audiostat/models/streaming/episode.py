import polars as pl
from audiostat.models.streaming.extended import ExtendedStreamingHistory


class EpisodeStreamingHistory:
    """Generic class representing extended audiobook streaming history."""

    schema: dict = {
        "ts": pl.Datetime,
        "ms_played": pl.Int64,
        "episode_name": pl.String,
        "episode_show_name": pl.String,
        "spotify_episode_uri": pl.String,
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
            pl.col("spotify_episode_uri").is_not_null()
        )
