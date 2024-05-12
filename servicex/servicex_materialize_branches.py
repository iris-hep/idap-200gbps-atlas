import argparse
import cProfile
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import awkward as ak
import dask
import dask_awkward as dak
from fspec_retry import register_retry_http_filesystem
import uproot
from dask.distributed import Client, LocalCluster, performance_report
from datasets import determine_dataset
from query_library import build_query

import servicex as sx


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
    ds_names: List[str],
    download: bool,
    query: Tuple[sx.FuncADLQuery, str],
) -> Dict[str, List[str]]:
    """Load and execute the servicex query. Returns a complete list of paths
    (be they local or url's) for the root or parquet files.
    """
    logging.info("Building ServiceX query")

    # Do the query.
    # TODO: Where is the enum that does DeliveryEnum come from?
    # TODO: Why does `Sample` fail type checking - that type ignore has already hidden one bug!
    # TODO: If I change Name after running, cache seems to fail (name doesn't track).
    # TODO: If you change the name of the item you'll get a multiple cache hit!
    # TODO: `servicex cache list` doesn't work and can't figure out how to make it work.
    # TODO: servicex_query_cache.json is being ignored (feature?)
    # TODO: Why does OutputFormat and delivery not work as enums? And fail typechecking with
    #       strings?
    # TODO: If some of these submissions work and others do not, we lose the ability to track the
    #       ones we fired off.
    #       an example is a title that is longer than 128 characters causes an immediate crash -
    #       but other queries
    #       already worked. Cache recovery @ the server would mean this wasn't important.
    # TODO: Would be nice if you didn't have to specify codegen at the top level, but just at
    #       the sample level (get an error if you move Codegen)
    spec = sx.ServiceXSpec(
        General=sx.General(
            ServiceX="atlasr22",
            Codegen=query[1],
            OutputFormat=sx.ResultFormat.root,  # type: ignore
            Delivery=("LocalCache" if download else "SignedURLs"),  # type: ignore
        ),
        Sample=[
            # TODO: Need a way to have the DID finder re-fetch the file list.
            sx.Sample(
                Name=f"speed_test_{ds_name}"[0:128],
                RucioDID=ds_name,
                Codegen=query[1],
                Query=query[0],
                NFiles=num_files,
                IgnoreLocalCache=ignore_cache,
            )  # type: ignore
            for ds_name in ds_names
        ],
    )
    for ds_name in ds_names:
        logging.info(f"Querying dataset {ds_name}")
    if num_files == 0:
        logging.info("Running on the full dataset(s).")
    else:
        logging.info(f"Running on {num_files} files of dataset.")

    logging.info("Starting ServiceX query")
    # TODO: When SX is queried for status, it always sends back the full
    #       qastle. This is way too much information for a long query
    #       like this.
    # TODO: async def get_transform_status(self, request_id: str) -> TransformStatus:
    #       needs to have a retry/backoff for when there is a timeout.
    # TODO: we should make servicex-app deployment scale based on time it takes
    #       to get response to a rest request.
    # TODO: Silent mode to suppress the marching ants progress.
    results = sx.deliver(spec)
    assert results is not None
    return results


def main(
    ignore_cache: bool = False,
    num_files: int = 10,
    dask_report: bool = False,
    ds_names: Optional[List[str]] = None,
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

    assert ds_names is not None
    dataset_files = query_servicex(
        ignore_cache=ignore_cache,
        num_files=num_files,
        ds_names=ds_names,
        download=download_sx_result,
        query=query,
    )

    for ds, files in dataset_files.items():
        logging.info(f"Dataset {ds} has {len(files)} files")
        assert len(files) > 0, "No files found in the dataset"

    # now materialize everything.
    logging.info(
        f"Using `uproot.dask` to open files (splitting files {steps_per_file} ways)."
    )
    # The 20 steps per file was tuned for this query and 8 CPU's and 32 GB of memory.
    logging.info("Starting build of DASK graphs")
    all_dask_data = {
        k: calculate_total_count(k, steps_per_file, files)
        for k, files in dataset_files.items()
    }
    logging.info("Done building DASK graphs.")

    # Do the calc now.
    logging.info("Computing the total count")
    all_tasks = {k: v[1] for k, v in all_dask_data.items()}
    if dask_report:
        with performance_report(filename="dask-report.html"):
            results = dask.compute(*all_tasks.values())  # type: ignore
            result_dict = dict(zip(all_tasks.keys(), results))
    else:
        results = dask.compute(*all_tasks.values())  # type: ignore
        result_dict = dict(zip(all_tasks.keys(), results))

    for k, r in result_dict.items():
        logging.info(f"{k}: result = {r:,}")

    # Scan through for any exceptions that happened during the dask processing.
    all_report_tasks = {k: v[0] for k, v in all_dask_data.items()}
    all_reports = dask.compute(*all_report_tasks.values())  # type: ignore
    for k, report_list in zip(all_report_tasks.keys(), all_reports):
        for process in report_list:
            if process.exception is not None:
                logging.error(
                    f"Exception in process '{process.message}' on file {process.args[0]} "
                    "for ds {k}"
                )


def calculate_total_count(
    ds_name: str, steps_per_file: int, files: List[str]
) -> Tuple[Any, Any]:
    """Calculate the non zero fields in the files.

    Args:
        steps_per_file (int): The number of steps to split the file into.
        files (List[str]): The list of files in which to count the fields.

    Returns:
        _: DASK graph for the total count.
    """
    data, report_to_be = uproot.dask(
        {f: "atlas_xaod_tree" for f in files},
        open_files=False,
        steps_per_file=steps_per_file,
        allow_read_errors_with_report=True,
    )

    # Now, do the counting.
    # The straight forward way to do this leads to a very large dask graph. We can
    # do a little prep work here and make it more clean.
    logging.debug(
        f"{ds_name}: Generating the dask compute graph for"
        f" {len(data.fields)} fields"  # type: ignore
    )

    total_count = 0
    assert isinstance(data, dak.Array)  # type: ignore
    for field in data.fields:
        logging.debug(f"{ds_name}: Counting field {field}")
        if str(data[field].type.content).startswith("var"):
            count = ak.count_nonzero(data[field], axis=-1)
            for _ in range(count.ndim - 1):  # type: ignore
                count = ak.count_nonzero(count, axis=-1)

            total_count = total_count + count  # type: ignore
        else:
            # We get a not implemented error when we try to do this
            # on leaves like run-number or event-number (e.g. scalars)
            # Maybe we should just be adding a 1. :-)
            logging.debug(
                f"{ds_name}: Field {field} is not a scalar field. Skipping count."
            )

    total_count = ak.count_nonzero(total_count, axis=0)

    n_optimized_tasks = len(dask.optimize(total_count)[0].dask)  # type: ignore
    logging.log(
        logging.INFO,
        f"{ds_name}: Number of tasks in the dask graph: optimized: "
        f"{n_optimized_tasks:,} "  # type: ignore
        f"unoptimized: {len(total_count.dask):,}",  # type: ignore
    )

    # total_count.visualize(optimize_graph=True)  # type: ignore
    # opt = Path("mydask.png")
    # opt.replace("dask-optimized.png")
    # total_count.visualize(optimize_graph=False)  # type: ignore
    # opt.replace("dask-unoptimized.png")

    return report_to_be, total_count


#     logging.info("Computing the total count")
#     if dask_report:
#         with performance_report(filename="dask-report.html"):
#             r, report_list = dask.compute(total_count, report_to_be)  # type: ignore
#     else:
#         r, report_list = dask.compute(total_count, report_to_be)  # type: ignore

#     logging.info(f"Done: result = {r:,}")

#     # Scan through for any exceptions that happened during the dask processing.
#     for process in report_list:
#         if process.exception is not None:
#             logging.error(
#                 f"Exception in process '{process.message}' on file {process.args[0]}"
#             )


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
  multi_1TB - All datasets between 1 and 2 TB.\n
  multi_small_10 - 10 small datasets (0.1 TB).\n
  multi_small_20 - 10 small datasets (0.1 TB).\n
  multi_data - All 4 large datasets.\n
  all - All the datasets.\n
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
        choices=[
            "data_50TB",
            "mc_1TB",
            "data_10TB",
            "multi_1TB",
            "all",
            "multi_small_10",
            "multi_small_20",
            "multi_data",
        ],
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
    client = None
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

    # Register fsspec special http retry filesystem
    register_retry_http_filesystem(client)

    # The steps per file needs to be adjusted for uproot 5.3.3 because of a small
    # bug - also because things start to get inefficient.
    if args.query == "xaod_small":
        steps_per_file = 1
    elif args.query == "xaod_medium":
        steps_per_file = 2

    ds_names = determine_dataset(args.dataset)

    # Build the query
    query = build_query(args.query)

    # Now run the main function
    if args.profile is False:
        main(
            ignore_cache=args.ignore_cache,
            num_files=args.num_files,
            dask_report=args.dask_profile,
            ds_names=ds_names,
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
