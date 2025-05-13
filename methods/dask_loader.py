import dask.dataframe as dd

def load_lazy(filepath):
    df = dd.read_csv(filepath)
    return df.compute()