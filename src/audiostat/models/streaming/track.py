import polars as pl
from audiostat.models.streaming.extended import ExtendedStreamingHistory


class TrackStreamingHistory:
    """Generic class representing extended audiobook streaming history."""

    schema: dict = {
        "ts": pl.Datetime,
        "ms_played": pl.Int64,
        "master_metadata_track_name": pl.String,
        "master_metadata_album_artist_name": pl.String,
        "master_metadata_album_album_name": pl.String,
        "spotify_track_uri": pl.String,
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
            pl.col("spotify_track_uri").is_not_null()
        )
