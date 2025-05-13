import polars as pl

def load_full(filepath):
    return pl.read_csv(filepath)