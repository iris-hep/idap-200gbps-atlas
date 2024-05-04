import argparse
import cProfile
import logging
import time
from pathlib import Path
from typing import List, Optional, Tuple

import awkward as ak
import dask
import dask_awkward as dak
import uproot
from dask.distributed import Client, LocalCluster, performance_report

import servicex as sx

from query_library import build_query


class ElapsedFormatter(logging.Formatter):
    """Logging formatter that adds an elapsed time record since it was
    first created. Error messages are printed relative to when the code
    started - which makes it easier to understand how long operations took.
    """

    def __init__(self, fmt="%(elapsed)s - %(levelname)s - %(name)s - %(message)s"):
        super().__init__(fmt)
        self._start_time = time.time()

    def format(self, record):
        record.elapsed = f"{time.time() - self._start_time:0>9.4f}"
        return super().format(record)


def query_servicex(
    ignore_cache: bool,
    num_files: int,
    ds_name: str,
    download: bool,
    query: Tuple[sx.FuncADLQuery, str],
) -> List[str]:
    """Load and execute the servicex query. Returns a complete list of paths
    (be they local or url's) for the root or parquet files.
    """
    logging.info("Building ServiceX query")
    logging.info(f"Using dataset {ds_name}.")
    if num_files == 0:
        logging.info("Running on the full dataset.")
    else:
        logging.info(f"Running on {num_files} files of dataset.")

    # Do the query.
    # TODO: Where is the enum that does DeliveryEnum come from?
    # TODO: Why does `Sample` fail type checking - that type ignore has already hidden one bug!
    # TODO: If I change Name after running, cache seems to fail (name doesn't track).
    # TODO: If you change the name of the item you'll get a multiple cache hit!
    # TODO: `servicex cache list` doesn't work and can't figure out how to make it work.
    # TODO: servicex_query_cache.json is being ignored (feature?)
    # TODO: Why does OutputFormat and delivery not work as enums? And fail typechecking with
    #       strings?
    spec = sx.ServiceXSpec(
        General=sx.General(
            ServiceX="atlasr22",
            Codegen=query[1],
            OutputFormat=sx.ResultFormat.root,  # type: ignore
            Delivery=("LocalCache" if download else "SignedURLs"),  # type: ignore
        ),
        Sample=[
            sx.Sample(
                Name=f"speed_test_{ds_name}",
                RucioDID=ds_name,
                Query=query[0],
                NFiles=num_files,
                IgnoreLocalCache=ignore_cache,
            )  # type: ignore
        ],
    )

    logging.info("Starting ServiceX query")
    results = sx.deliver(spec)
    assert results is not None
    return results[f"speed_test_{ds_name}"]


def main(
    ignore_cache: bool = False,
    num_files: int = 10,
    dask_report: bool = False,
    ds_name: Optional[str] = None,
    download_sx_result: bool = False,
    steps_per_file: int = 3,
    query: Optional[Tuple[sx.FuncADLQuery, str]] = None,
):
    """Match the operations found in `materialize_branches` notebook:
    Load all the branches from some dataset, and then count the flattened
    number of items, and, finally, print them out.
    """
    assert query is not None, "No query provided to run."

    # Make sure there is a file here to save the SX query ID's to
    # improve performance!
    sx_query_ids = Path("./servicex_query_cache.json")
    if not sx_query_ids.exists():
        sx_query_ids.touch()

    assert ds_name is not None
    files = query_servicex(
        ignore_cache=ignore_cache,
        num_files=num_files,
        ds_name=ds_name,
        download=download_sx_result,
        query=query,
    )

    assert len(files) > 0, "No files found in the dataset"
    for i, f in enumerate(files):
        logging.debug(f"{i:00}: {f}")

    # now materialize everything.
    logging.info(
        f"Using `uproot.dask` to open files (splitting files {steps_per_file} ways)."
    )
    # The 20 steps per file was tuned for this query and 8 CPU's and 32 GB of memory.
    data, report_to_be = uproot.dask(
        {f: "atlas_xaod_tree" for f in files},
        open_files=False,
        steps_per_file=steps_per_file,
        allow_read_errors_with_report=True,
    )

    # Now, do the counting.
    logging.info(
        f"Generating the dask compute graph for {len(data.fields)} fields"  # type: ignore
    )

    # The straight forward way to do this leads to a very large dask graph. We can
    # do a little prep work here and make it more clean.
    total_count = 0
    assert isinstance(data, dak.Array)  # type: ignore
    for field in data.fields:
        logging.debug(f"Counting field {field}")
        if str(data[field].type.content).startswith("var"):
            count = ak.count_nonzero(data[field], axis=-1)
            for _ in range(count.ndim - 1):  # type: ignore
                count = ak.count_nonzero(count)

            total_count = total_count + count  # type: ignore
        else:
            # We get a not implemented error when we try to do this
            # on leaves like run-number or event-number (e.g. scalars)
            # Maybe we should just be adding a 1. :-)
            logging.info(f"Field {field} is not a scalar field. Skipping count.")

    total_count = ak.count_nonzero(total_count, axis=0)

    # Do the calc now.
    logging.info(
        "Number of tasks in the dask graph: optimized: "
        f"{len(dask.optimize(total_count)[0].dask):,} "  # type: ignore
        f"unoptimized {len(total_count.dask):,}"  # type: ignore
    )
    # total_count.visualize(optimize_graph=True)  # type: ignore
    logging.info("Computing the total count")
    if dask_report:
        with performance_report(filename="dask-report.html"):
            r = total_count.compute()  # type: ignore
    else:
        r = total_count.compute()  # type: ignore

    logging.info(f"Done: result = {r:,}")

    # Scan through for any exceptions that happened during the dask processing.
    report_list = report_to_be.compute()
    for process in report_list:
        if process.exception is not None:
            logging.error(
                f"Exception in process '{process.message}' on file {process.args[0]}"
            )


if __name__ == "__main__":
    # This block of code just setups for the run (command line arguments, logging, etc)

    # Create the argument parser
    parser = argparse.ArgumentParser(
        description="Run simple ServiceX query and processing",
        epilog="""
Note on the dataset argument: \n
\n
  data_50TB - 50 TB of data from dta18. 64803 files.\n
  mc_1TB - 1.2 TB of data from mc20. 232 files.\n
  data_10TB - 10 TB of data from data15. 10049 files.\n
""",
    )

    # Add the verbosity flag
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase output verbosity"
    )

    # Add the flag to disable servicex cache
    parser.add_argument(
        "--ignore-cache", action="store_true", help="Disable ServiceX cache"
    )

    parser.add_argument(
        "--profile",
        action="store_true",
        help="Enable profiling of the code. This will output a file "
        "called `sx_materialize_branches.pstats`.",
    )

    parser.add_argument(
        "--dask-profile",
        action="store_true",
        help="Enable profiling of the Dask execution. This will output a file "
        "called `dask-report.html`.",
    )

    parser.add_argument(
        "--download-sx-result",
        action="store_true",
        help="Download the result from ServiceX. If not specified, the result will be "
        "read directly from SX's S3 instance. Likely only used during remote debugging.",
    )

    # Add the flag to enable/disable local Dask cluster
    parser.add_argument(
        "--distributed-client",
        choices=["local", "none", "scheduler"],
        default="local",
        help="Specify the type of Dask cluster to enable (default: local uses 8 cores "
        "in process, none doesn't use any)",
    )

    # Add a flag for different datasets
    parser.add_argument(
        "--dataset",
        choices=["data_50TB", "mc_1TB", "data_10TB"],
        default="mc_1TB",
        help="Specify the dataset to use",
    )

    # Add a flag for three queries - xaod_all, xaod_medium, xaod_small:
    parser.add_argument(
        "--query",
        choices=["xaod_all", "xaod_medium", "xaod_small"],
        default="xaod_all",
        help="Specify the query to use. Defaults to xao_all.",
    )

    # Add the flag to specify the Dask scheduler address
    parser.add_argument(
        "--dask-scheduler",
        help="Specify the address of the Dask scheduler. Only valid when distributed-client "
        "is 'scheduler'.",
        default=None,
    )

    # Number of files in the dataset to run on. Default to 10. Specify 0 to run on full.
    parser.add_argument(
        "--num-files",
        type=int,
        default=10,
        help="Number of files in the dataset to run on. Default to 10. Specify 0 to run on full.",
    )

    # Parse the command line arguments
    args = parser.parse_args()

    # Create a handler, set the formatter to it, and add this handler to the logger
    handler = logging.StreamHandler()
    handler.setFormatter(ElapsedFormatter())
    root_logger = logging.getLogger()

    # Set the logging level based on the verbosity flag.
    # make sure the time comes out so people can "track" what is going on.
    if args.verbose == 1:
        root_logger.setLevel(level=logging.INFO)
    elif args.verbose >= 2:
        root_logger.setLevel(level=logging.DEBUG)
    else:
        root_logger.setLevel(level=logging.WARNING)
    root_logger.addHandler(handler)

    # Keep the log quiet when we want it to be.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    # Create the client dask worker
    steps_per_file = 1
    if args.distributed_client == "local":
        # Do not know how to do it otherwise.
        n_workers = 8
        logging.debug("Creating local Dask cluster for {n_workers} workers")
        cluster = LocalCluster(
            n_workers=n_workers, processes=False, threads_per_worker=1
        )
        client = Client(cluster)
        steps_per_file = 20
    elif args.distributed_client == "scheduler":
        logging.debug("Connecting to Dask scheduler at {scheduler_address}")
        assert args.dask_scheduler is not None
        client = Client(args.dask_scheduler)
        steps_per_file = 2

    # The steps per file needs to be adjusted for uproot 5.3.3 because of a small
    # bug - also because things start to get inefficient.
    if args.query == "xaod_small":
        steps_per_file = 1
    elif args.query == "xaod_medium":
        steps_per_file = 2

    # Determine the dataset
    ds_name = (
        "data15_13TeV.periodAllYear.physics_Main.PhysCont.DAOD_PHYSLITE.grp15_v01_p6026"
        if args.dataset == "mc_10TB"
        else (
            "data18_13TeV.periodAllYear.physics_Main.PhysCont.DAOD_PHYSLITE.grp18_v01_p6026"
            if args.dataset == "data_50TB"
            else (
                "mc20_13TeV.364157.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV0_70_CFilterBVeto"
                ".deriv.DAOD_PHYSLITE.e5340_s3681_r13145_p6026"
                if args.dataset == "mc_1TB"
                else None
            )
        )
    )
    assert ds_name is not None, "Invalid/unknown dataset specified"

    # Build the query
    query = build_query(args.query)

    # Now run the main function
    if args.profile is False:
        main(
            ignore_cache=args.ignore_cache,
            num_files=args.num_files,
            dask_report=args.dask_profile,
            ds_name=ds_name,
            download_sx_result=args.download_sx_result,
            steps_per_file=steps_per_file,
            query=query,
        )
    else:
        cProfile.run(
            "main(ignore_cache=args.ignore_cache, num_files=args.num_files, "
            "dask_report=args.dask_profile, ds_name = ds_name, "
            "download_sx_result=args.download_sx_result, steps_per_file=steps_per_file"
            "query=query)",
            "sx_materialize_branches.pstats",
        )
        logging.info("Profiling data saved to `sx_materialize_branches.pstats`")
