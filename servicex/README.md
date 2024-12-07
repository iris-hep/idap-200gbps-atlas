# Introduction

This directory contains scripts and notebooks to implement fetching the data locally using ServiceX.

The default `servicex.yaml` file was used from the UChicago AF.

## Files

| File | Description |
|------|-------------|
| 00-exploring-the-data | Outlines the raw ServiceX code that we can use. We'll need to develop libraries which will obscure this code quite a bit given how many branches we'll need to load. Working around [this bug](https://github.com/dask-contrib/dask-awkward/issues/456) makes the code a little more complex than it needs to be.  |
| 01-test_servicex_client_v3.ipynb | Similar to 00, but uses the 3.0 interface. |
| materalize_branches-servicex-3.ipynb | Work in progress running many different types of queries with different backend options |
| servicex_materialize_branches.py | Command line script that will load and spin through in a simple way branches from a PHYSLITE dataset. Use `--help` to see options |

## Running a `materialize` test

The following tests were used to run a performance test using the `servicex_materialize_branches.py`:

1. Create a JupyterHub cluster on UChicago:
    1. Create the jupyterlab instance
        * Use the `AB-dev` image
        * 16 GB of memory
        * 8 cores
    1. Open up the JupyterLab interface
    2. Create a DASK cluster (and capture the address)
        * Use the `dask` side-bar left-column in the jupyterlab interface.
        * At the bottom click "create"
            * While you can configure what you need, the default (1000 max) is just fine.
        * Copy the scheduler address that appears in the new cluster.
            * You'll need the `tcp://dask-xxxxxx-xxxxxx-x.af-jupyter:8786` like address.
            * Easiest way to get this is by creating an empty notebook, and then dragging the DASK cluster tile onto the notebook. It will create a cell with the address in it!
3. Run the materialize bench mark:
    1. Create a terminal window
    1. Make sure that the packages needed are up to date
        * As of this, just do `pip install servicex==3.0.1-rc1`  
    * `python servicex/servicex_materialize_branches.py --distributed-client scheduler --dask-scheduler 'tcp://dask-xxxxxx-xxxxxxxx-x.af-jupyter:8786' --num-files 0 --query xaod_small`
        * Add `--ignore-cache` if need be
        * Use `--help` to get a full list of things
        * Drop `--num-files 0` if you want to test more than 10 files per dataset.
        * Add `--dataset all` to run on all datasets.
    * The `-v` turns on a first level of verbosity which dumps out timings to `stdout`.
    * The second two are required to run with a full `dask` scheduler.
    * For stress testing on uchicago, I've been using `python servicex/servicex_materialize_branches.py --distributed-client scheduler --dask-scheduler 'tcp://dask-xxx-xxxxxxxx-x.af-jupyter:8786' --query xaod_small --ignore-cache --dataset all`
        * This queues up some 200 datasets, not all of which have files any more!
        * Seems to hang for days in the front end, though the back end seems to run a number of the transforms (seems to).

## Notes on using `servicex_materialize_branches.py`

This script is basic and mostly hard-coded. There are a few command line options that are useful:

### `-v`

Gives you a nice high level dump:

```text
PS C:\Users\gordo\Code\iris-hep\idap-200gbps-atlas> python .\servicex\servicex_materialize_branches.py -v
0000.0000 - INFO - Using release 21.2.231
0000.0010 - INFO - Building ServiceX query
0000.0471 - WARNING - Fetched the default calibration configuration for a query. It should have been intentionally configured - using configuration for data format PHYS
0000.1173 - INFO - Starting ServiceX query
0000.2509 - INFO - Finished ServiceX query
0000.2519 - INFO - Using `uproot.dask` to open files
0000.2960 - INFO - Generating the dask compute graph for 7 fields
0000.3357 - INFO - Computing the total count
0017.1230 - INFO - Done: result = 59,779,410
```

### `--distributed-client`

Use `none` and it will not set up any sort of distributed `dask` client, which likely means it will run single threaded. If you use `local`, then it will create a local in-process distributed client, that will have the same number of workers as there are cores (as determined by python's `multiprocessing` package)

When using `local`, it is hardcoded to use 8 cores. This works for a notebook allocated with 32 GB and 8 cores, and `steps_per_file=20`. There is hand tuning here, which I hope will eventually not be required!

### `--sx-name`

This allows you to run on other frontend's besides the typical `servicex-uc-af`. Just make sure the end points are listed in your `servicex.yaml` or `.servicex` file!

### `--profile`

This will write out a `sx_materialize_branches.pstats` file, which you can then run `snakeviz` on.

## Running on UChicago JupyterLab

Using the image `AB-dev`:

1. Get `servicex.yaml` copied somewhere it will be picked up.
2. If you need a newer version of the serviceX client than what

```
servicex --version
```

shows, install a development version from GitHub directly with

```
python -m pip install --upgrade 'git+https://github.com/ssl-hep/ServiceX_frontend@3.0_develop'
```
