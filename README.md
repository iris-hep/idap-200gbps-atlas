# IDAP 200 Gbps with ATLAS PHYSLITE

Targeting analysis at 200 Gbps with ATLAS PHYSLITE. This repository is very much a work in progress.

ATLAS does not have released OpenData, so there isn't an AGC we can copy and try to run. As a result, this repository's main purpose is as a facilities test:

* Run from PHYSLITE
* Load 200 Gbps off of the PHYSLITE samples
* Push all that data downstream to DASK (or similar) workers.

## Description of files

* `size_per_branch.ipynb`: produce breakdown of branch sizes for given file
* `branch_sizes.json`: output of , produced by `size_per_branch.ipynb`
* `materialize_branches.ipynb`: read list of branches, distributable with Dask (use for benchmarking)

## Usage

When run on the UChicago AF Jupyter Notebook no package installs are required.

There is a `requirements.txt` which should allow this to be run on a bare-bones machine (modulo location of files, etc.).

If you are going to use the `servicex` version, you have to pin `dask_awkward==2024.2.0`. The future versions have a [bug](https://github.com/dask-contrib/dask-awkward/issues/456) which hasn't been fixed yet.

## Input file details

The folder `input_files` contains the list of input containers / files and related metadata plus scripts to produce these.

In total:

* number of files: 218,960
* size: 191.022 TB
* number of events: 23,344,277,104

with additional files:

* `input_files/find_containers.py`: query rucio for a list of containers, given a list of (hardcoded) DSIDs
* `input_files/container_list.txt`: list of containers to run over
* `input_files/produce_container_metadata.py`: query metadata for containers: number of files / events, size
* `input_files/container_metadata.json`: output of `input_files/produce_container_metadata.py` with container metadata
* `input_files/get_file_list.py`: for a given dataset creates a txt file listing file access paths that include apropriate xcache. The same kind of output can be obtained by doing:

    ```
    export SITE_NAME=AF_200
    rucio list-file-replicas mc20_13TeV:mc20_13TeV.364126.Sherpa_221_NNPDF30NNLO_Zee_MAXHTPTV500_1000.deriv.DAOD_PHYSLITE.e5299_s3681_r13145_p6026 --protocol root  --pfns --rses MWT2_UC_LOCALGROUPDISK
    ```

## Acknowledgements

[![NSF-1836650](https://img.shields.io/badge/NSF-1836650-blue.svg)](https://nsf.gov/awardsearch/showAward?AWD_ID=1836650)
[![PHY-2323298](https://img.shields.io/badge/PHY-2323298-blue.svg)](https://nsf.gov/awardsearch/showAward?AWD_ID=2323298)

This work was supported by the U.S. National Science Foundation (NSF) cooperative agreements [OAC-1836650](https://nsf.gov/awardsearch/showAward?AWD_ID=1836650) and [PHY-2323298](https://nsf.gov/awardsearch/showAward?AWD_ID=2323298) (IRIS-HEP).
