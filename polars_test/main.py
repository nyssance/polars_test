from pathlib import Path

import polars as pl


def main():
    file = Path(__file__).resolve().parent.parent / "static/PX9999-1d.csv"
    print(f"Read day kline \n<file://{file}>")
    df_day = pl.read_csv(file, try_parse_dates=True)
    print(f"Generate month/quarter/year kline")
    for interval in ["1mo", "1q", "1y"]:
        output = Path(str(file).replace("1d", interval))
        print(f"<file://{output}>")
        df_day.group_by_dynamic("date", every=interval).agg(
            pl.col("date").last().alias("last_date"),
            pl.col("open").first().alias("open"),
            pl.col("close").last().alias("close"),
            pl.col("low").min().alias("low"),
            pl.col("high").max().alias("high"),
            pl.col("volume").sum().alias("volume"),
            pl.col("money").sum().alias("money")
        ).select(
            pl.exclude("date")
        ).rename(
            {"last_date": "date"}
        ).write_csv(output)


if __name__ == "__main__":
    main()
