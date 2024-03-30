# IDAP 200 Gbps with ATLAS PHYSLITE

Targeting analysis at 200 Gbps with ATLAS PHYSLITE. This repository is very much a work in progress.

ATLAS does not have released OpenData, so there isn't a AGC we can copy and try to run. As a result, this repository's main purpose is as a facilities test:

* Run from PHYSLITE
* Load 200 Gbps off of the PHYSLITE samples
* Push all that data downstream to DASK (or similar) workers.

## Description of files

- `size_per_branch.ipynb`: produce breakdown of branch sizes for given file
- `branch_sizes.json`: output of , produced by `size_per_branch.ipynb`
- `materialize_branches.ipynb`: read list of branches, distributable with Dask (use for benchmarking)

## Usage

WHen run on the UChicago AF Jupyter Notebook no package installs are required.

There is a `requirements.txt` which should allow this to be run on a bare-bones machine (modulo location of files, etc.).

If you are going to use the `servicex` version, you have to pin `dask_awkward==2024.2.0`. The future versions have a [bug](https://github.com/dask-contrib/dask-awkward/issues/456) which hasn't been fixed yet.

## Acknowledgements

[![NSF-1836650](https://img.shields.io/badge/NSF-1836650-blue.svg)](https://nsf.gov/awardsearch/showAward?AWD_ID=1836650)
[![PHY-2323298](https://img.shields.io/badge/PHY-2323298-blue.svg)](https://nsf.gov/awardsearch/showAward?AWD_ID=2323298)

This work was supported by the U.S. National Science Foundation (NSF) cooperative agreements [OAC-1836650](https://nsf.gov/awardsearch/showAward?AWD_ID=1836650) and [PHY-2323298](https://nsf.gov/awardsearch/showAward?AWD_ID=2323298) (IRIS-HEP).
