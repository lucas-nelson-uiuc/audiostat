import marimo

__generated_with = "0.14.10"
app = marimo.App(width="full")


@app.cell
def _():
    import polars as pl

    return (pl,)


@app.cell
def _():
    from audiostat.models.streaming import (
        ExtendedStreamingHistory,
        TrackStreamingHistory,
        AudiobookStreamingHistory,
        EpisodeStreamingHistory,
    )

    return (
        AudiobookStreamingHistory,
        EpisodeStreamingHistory,
        ExtendedStreamingHistory,
        TrackStreamingHistory,
    )


@app.cell
def _():
    from pathlib import Path

    STREAMING_HISTORY: str = (
        "/Users/lucasnelson/Desktop/Data/Spotify Extended Streaming History"
    )
    return Path, STREAMING_HISTORY


@app.cell
def _(ExtendedStreamingHistory, Path, STREAMING_HISTORY: str):
    data = ExtendedStreamingHistory.loader(
        *Path(STREAMING_HISTORY).glob("Streaming_History_Audio*.json")
    )
    return (data,)


@app.cell
def _(TrackStreamingHistory, data):
    TrackStreamingHistory.loader(data=data)
    return


@app.cell
def _(AudiobookStreamingHistory, data):
    AudiobookStreamingHistory.loader(data=data)
    return


@app.cell
def _(EpisodeStreamingHistory, data):
    EpisodeStreamingHistory.loader(data=data)
    return


@app.cell
def _(pl):
    def popular_tracks(data: pl.DataFrame, year: int) -> pl.DataFrame:
        return (
            data.filter(pl.col("ts").dt.year() == pl.lit(year))
            .group_by("master_metadata_album_artist_name", "master_metadata_track_name")
            .agg(pl.len(), pl.sum("ms_played"))
            .sort(by="ms_played", descending=True)
        )

    return (popular_tracks,)


@app.cell
def _(TrackStreamingHistory, data, popular_tracks):
    popular_tracks(data=TrackStreamingHistory.loader(data=data), year=2022)
    return


@app.cell
def _(TrackStreamingHistory, data, popular_tracks):
    popular_tracks(data=TrackStreamingHistory.loader(data=data), year=2023)
    return


@app.cell
def _(TrackStreamingHistory, data, popular_tracks):
    popular_tracks(data=TrackStreamingHistory.loader(data=data), year=2024)
    return


@app.cell
def _(TrackStreamingHistory, data, popular_tracks):
    popular_tracks(data=TrackStreamingHistory.loader(data=data), year=2025)
    return


if __name__ == "__main__":
    app.run()
