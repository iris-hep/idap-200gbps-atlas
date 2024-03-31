# Introduction

This directory contains scripts and notebooks to implement fetching the data locally using ServiceX.

The default `servicex.yaml` file was used from the UChicago AF.

Note that you'll need to be aware of the `requirements.txt` as a bug in `dask_awkward` means this can't run on the most recent version.

## Files

| File | Description |
|------|-------------|
| 00-exploring-the-data | Outlines the raw ServiceX code that we can use. We'll need to develop libraries which will obscure this code quite a bit given how many branches we'll need to load. This notebook can't run on the most recent version of `dask_awkward` - until [this bug](https://github.com/dask-contrib/dask-awkward/issues/456) is fixed. |
