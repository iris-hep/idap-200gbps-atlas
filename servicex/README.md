# Introduction

This directory contains scripts and notebooks to implement fetching the data locally using ServiceX.

The default `servicex.yaml` file was used from the UChicago AF.

## Files

| File | Description |
|------|-------------|
| 00-exploring-the-data | Outlines the raw ServiceX code that we can use. We'll need to develop libraries which will obscure this code quite a bit given how many branches we'll need to load. Working around [this bug](https://github.com/dask-contrib/dask-awkward/issues/456) makes the code a little more complex than it needs to be. |
| servicex_materialize_branches.py | Command line script that will load and spin through in a simple way branches from a PHYSLITE dataset. Use `--help` to see options |

## Running a `materialize` test

The following tests were used to run a performance test using the `servicex_materialize_branches.py`:

1. Create a JupyterHub cluster on UChicago:
    1. Create the j-lab instance
        * Use the `AB-dev` image
        * 32 GB of memory
        * 8 cores
        * From the command line inside the instance, issue the command `pip install 'func_adl_servicex_xaodr22>=2.0a1'`
    2. Once started create a DASK cluster
        * Use the `dask` web page
        * At the bottom click "create"
        * Copy the scheduler address that appears in the new cluster.
    3. Run the materialize bench mark:
        * `python servicex/servicex_materialize_branches.py -v --distributed-client scheduler --dask-scheduler 'tcp://dask-gwatts-51a0bb30-c.af-jupyter:8786' --num-files 0`
        * The `-v` turns on a first level of verbosity which dumps out timings to `stdout`.
        * The second two are required to run with a full `dask` scheduler.

## Notes on using `servicex_materialize_branches.py

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

### `--profile`

This will write out a `sx_materialize_branches.pstats` file, which you can then run `snakeviz` on.

## Running on UChicago JuppterLab

Using the image `AB-dev`

1. `pip install 'func_adl_servicex_xaodr22>=2.0a1'`
1. Get `servicex.yaml` copied somewhere it will be picked up.
