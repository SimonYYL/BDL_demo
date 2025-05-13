import pandas as pd

def load_full(file_path):
    return pd.read_csv(file_path)

def load_chunked(file_path, chunksize):
    chunks = []
    for chunk in pd.read_csv(file_path, chunksize=chunksize):
        chunks.append(chunk)
    return pd.concat(chunks, ignore_index=True)