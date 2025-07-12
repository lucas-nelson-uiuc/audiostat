import json
import polars as pl


class Playlist:
    """Generic class representing playlist data."""

    schema: dict = {
        "name": pl.String,
        "lastModifiedDate": pl.String,
        "items": pl.List(
            pl.Struct(
                [
                    pl.Field(
                        name="track",
                        dtype=pl.Struct(
                            [
                                pl.Field(
                                    name="trackName",
                                    dtype=pl.String,
                                ),
                                pl.Field(
                                    name="artistName",
                                    dtype=pl.String,
                                ),
                                pl.Field(
                                    name="albumName",
                                    dtype=pl.String,
                                ),
                                pl.Field(
                                    name="trackUri",
                                    dtype=pl.String,
                                ),
                            ]
                        ),
                    ),
                    pl.Field(name="episode", dtype=pl.Null),
                    pl.Field(name="localTrack", dtype=pl.Null),
                    pl.Field(name="addedData", dtype=pl.String),
                ]
            )
        ),
        "description": pl.String,
        "numberOfFollowers": pl.Int32,
    }

    @classmethod
    def loader(cls, *source: str) -> pl.DataFrame:
        def _loader(source: str) -> pl.DataFrame:
            with open(source) as fp:
                data = json.load(fp)
            return pl.json_normalize(data["playlists"], schema=cls.schema)

        return pl.concat(map(_loader, source)).with_columns(
            pl.col("lastModifiedDate").str.to_date(format="%Y-%m-%d")
        )
