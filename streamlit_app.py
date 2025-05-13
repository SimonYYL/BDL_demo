import streamlit as st
import time
import pandas as pd
import dask.dataframe as dd
import polars as pl
import tempfile
import os
from multiprocessing import Pool
import io
from methods import pandas_loader, polars_loader, dask_loader

st.title("Big File Loader Benchmark")
update_file = st.file_uploader("Put your BIG files here:")
method = st.selectbox("Selected loading method" , [
    "Pandas - Full Load",
    "Pandas - Chunking",
    "Polars - Full Load",
    "Dask - Lazy Read"
    ])

# op
if method == 'Pandas - Chunking':
    chunk_size = st.number_input(
        label="Chunk Size",
        min_value=1000,
        value=100000,
        step=10000,
    )

# start buttton
if update_file and st.button("Run Benchmark"):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(update_file.read())
        tmp_path = tmp.name
    st.write(f"**Method:** {method}")
    start = time.time()
    if method == 'Pandas - Full Load':
        df = pandas_loader(tmp_path)
    elif method == 'Pandas - Chunking':
        df = pandas_loader(tmp_path, chunk_size=chunk_size)
    elif method == 'Polars - Full Load':
        df = polars_loader(tmp_path)
    elif method == 'Dask - Lazy Read':
        df = dask_loader(tmp_path)
    else:
        st.write("Invalid method selected.")
        st.stop()

    end = time.time()
    st.success(f"Loading completed in {end - start:.2f} seconds")
    st.write("### Preview of the loaaded data:")
    st.dataframe(df.head(10))

    os.remove(tmp_path)  # Clean up the temporary file

st.markdown("---")
st.markdown("This app is a benchmark for loading big files using different methods. ")
