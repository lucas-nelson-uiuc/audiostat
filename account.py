import marimo

__generated_with = "0.14.10"
app = marimo.App(width="full")


@app.cell
def _():
    import polars as pl
    from pathlib import Path

    ACCOUNT_DATA: str = "/Users/lucasnelson/Desktop/spotify/Spotify Account Data/"

    for path in list(Path(ACCOUNT_DATA).iterdir()):
        try:
            print(path)
            print(pl.read_json(path).head(5))
        except Exception:
            continue
    return ACCOUNT_DATA, Path, pl


@app.cell
def _(pl):
    pl.read_json(
        "/Users/lucasnelson/Desktop/spotify/Spotify Account Data/YourLibrary.json"
    )
    return


@app.cell
def _(pl):
    PLAYLIST_SCHEMA: dict = {
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
    return (PLAYLIST_SCHEMA,)


@app.cell
def _(ACCOUNT_DATA: str, PLAYLIST_SCHEMA: dict, Path, pl):
    import json

    def loader(source: str) -> pl.DataFrame:
        with open(source) as fp:
            data = json.load(fp)
        return pl.json_normalize(
            data["playlists"], schema=PLAYLIST_SCHEMA
        ).with_columns(pl.col("lastModifiedDate").str.to_date(format="%Y-%m-%d"))

    files = Path(ACCOUNT_DATA).rglob("Playlist*json")
    pl.concat(map(loader, files))
    return


if __name__ == "__main__":
    app.run()
