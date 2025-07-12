import polars as pl


class ExtendedStreamingHistory:
    """Generic class representing extended streaming history table."""

    path: str = "Streaming_History_Audio*.json"
    schema: dict = {
        "ts": pl.Datetime,
        "ms_played": pl.Int64,
        "master_metadata_track_name": pl.String,
        "master_metadata_album_artist_name": pl.String,
        "master_metadata_album_album_name": pl.String,
        "spotify_track_uri": pl.String,
        "episode_name": pl.String,
        "episode_show_name": pl.String,
        "spotify_episode_uri": pl.String,
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
    def loader(cls, *source: str) -> pl.DataFrame:
        def _loader(source: str) -> pl.DataFrame:
            return pl.read_json(source, schema=cls.schema)

        return pl.concat(map(_loader, source))
