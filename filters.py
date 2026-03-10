import polars as pl

def only_dark(df):
    df = df.filter(pl.col('exchange') == 4)
    return df

def only_lit(df):
    df = df.filter(pl.col('exchange') != 4)
    return df

