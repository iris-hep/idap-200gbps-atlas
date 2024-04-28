import argparse
import cProfile
import logging
from pathlib import Path
import time
from typing import List, Optional

import awkward as ak
import uproot
from dask.distributed import Client, LocalCluster, performance_report
from func_adl_servicex_xaodr22 import (
    SXDSAtlasxAODR22PHYSLITE,
    atlas_release,
    cpp_vfloat,
    cpp_float,
    cpp_int,
    cpp_vint,
)

from servicex import ServiceXDataset


class ElapsedFormatter(logging.Formatter):
    """Logging formatter that adds an elapsed time record since it was
    first created. Error messages are printed relative to when the code
    started - which makes it easier to understand how long operations took.
    """

    def __init__(self, fmt="%(elapsed)s - %(levelname)s - %(message)s"):
        super().__init__(fmt)
        self._start_time = time.time()

    def format(self, record):
        record.elapsed = f"{time.time() - self._start_time:0>9.4f}"
        return super().format(record)


def query_servicex(
    ignore_cache: bool, num_files: int, ds_name: str, download: bool
) -> List[str]:
    """Load and execute the servicex query. Returns a complete list of paths
    (be they local or url's) for the root or parquet files.
    """
    logging.info("Building ServiceX query")
    logging.info(f"Using dataset {ds_name}.")

    # Build the data query for SX
    files_postfix = "" if num_files == 0 else f"?files={num_files}"
    rucio_ds = f"rucio://{ds_name}{files_postfix}"

    # Because we are going to do a specialized query, we'll alter the return type here.
    ds = SXDSAtlasxAODR22PHYSLITE(rucio_ds, backend="atlasr22")
    ds.return_qastle = True

    # Build the query
    # TODO: The EventInfo argument should default correctly
    #       (that may just be a matter of using func_adl xaod r22)
    # TODO: dataclass should be supported so as not to lose type-following!
    # TODO: once https://github.com/iris-hep/func_adl/issues/136 fixed, turn off fmt off.
    # fmt: off
    query = (ds.Select(lambda e: {
            "evt": e.EventInfo("EventInfo"),
            "jet": e.Jets(),
        })
        .Select(lambda ei: {
            "event_number": ei.evt.eventNumber(),  # type: ignore
            "run_number": ei.evt.runNumber(),  # type: ignore
            "jet_pt": ei.jet.Select(lambda j: j.pt() / 1000),  # type: ignore
            "jet_eta": ei.jet.Select(lambda j: j.eta()),  # type: ignore
            "jet_phi": ei.jet.Select(lambda j: j.phi()),  # type: ignore
            "jet_m": ei.jet.Select(lambda j: j.m()),  # type: ignore
            "jet_EnergyPerSampling":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_vfloat]("EnergyPerSampling")
                ),
            "jet_SumPtTrkPt500":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_vfloat]("SumPtTrkPt500")
                ),
            "jet_TrackWidthPt1000":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_vfloat]("TrackWidthPt1000")
                ),
            "jet_NumTrkPt500":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_vint]("NumTrkPt500")
                ),
            "jet_NumTrkPt1000":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_vint]("NumTrkPt1000")
                ),
            "jet_SumPtChargedPFOPt500":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_vfloat]("SumPtChargedPFOPt500")
                ),
            "jet_Timing":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("Timing")
                ),
            "jet_JetConstitScaleMomentum_eta":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("JetConstitScaleMomentum_eta")
                ),
            "jet_ActiveArea4vec_eta":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("ActiveArea4vec_eta")
                ),
            "jet_DetectorEta":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("DetectorEta")
                ),
            "jet_JetConstitScaleMomentum_phi":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("JetConstitScaleMomentum_phi")
                ),
            "jet_ActiveArea4vec_phi":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("ActiveArea4vec_phi")
                ),
            "jet_JetConstitScaleMomentum_m":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("JetConstitScaleMomentum_m")
                ),
            "jet_JetConstitScaleMomentum_pt":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("JetConstitScaleMomentum_pt")
                ),
            "jet_Width":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("Width")
                ),
            "jet_EMFrac":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("EMFrac")
                ),
            "jet_ActiveArea4vec_m":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("ActiveArea4vec_m")
                ),
            "jet_DFCommonJets_QGTagger_TracksWidth":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("DFCommonJets_QGTagger_TracksWidth")
                ),
            "jet_JVFCorr":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("JVFCorr")
                ),
            "jet_DFCommonJets_QGTagger_TracksC1":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("DFCommonJets_QGTagger_TracksC1")
                ),
            "jet_PSFrac":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("PSFrac")
                ),
            "jet_DFCommonJets_QGTagger_NTracks":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_int]("DFCommonJets_QGTagger_NTracks")
                ),
            "jet_DFCommonJets_fJvt":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("DFCommonJets_fJvt")
                ),
            "jet_PartonTruthLabelID":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_int]("PartonTruthLabelID")
                ),
            "jet_HadronConeExclExtendedTruthLabelID":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_int]("HadronConeExclExtendedTruthLabelID")
                ),
            "jet_ConeTruthLabelID":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_int]("ConeTruthLabelID")
                ),
            "jet_HadronConeExclTruthLabelID":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_int]("HadronConeExclTruthLabelID")
                ),
            "jet_ActiveArea4vec_pt":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("ActiveArea4vec_pt")
                ),
        })
    )
    # fmt: on

    # Do the query.
    ds_prime = ServiceXDataset(
        rucio_ds, backend_name="atlasr22", ignore_cache=ignore_cache
    )
    logging.info("Starting ServiceX query")
    if download:
        files = ds_prime.get_data_rootfiles(query.value(), title="First Request")
        logging.info("Finished ServiceX query")
        return [str(f) for f in files]
    else:
        files = ds_prime.get_data_rootfiles_uri(
            query.value(), title="First Request", as_signed_url=True
        )
        logging.info("Finished ServiceX query")
        return [str(f.url) for f in files]


def main(
    ignore_cache: bool = False,
    num_files: int = 10,
    dask_report: bool = False,
    ds_name: Optional[str] = None,
    download_sx_result: bool = False,
):
    """Match the operations found in `materialize_branches` notebook:
    Load all the branches from some dataset, and then count the flattened
    number of items, and, finally, print them out.
    """
    logging.info(f"Using release {atlas_release} for type information.")

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
    )

    assert len(files) > 0, "No files found in the dataset"
    for i, f in enumerate(files):
        logging.debug(f"{i:00}: {f}")

    # now materialize everything.
    logging.info("Using `uproot.dask` to open files")
    # The 20 steps per file was tuned for this query and 8 CPU's and 32 GB of memory.
    data, report_to_be = uproot.dask(
        {f: "atlas_xaod_tree" for f in files},
        open_files=False,
        steps_per_file=20,
        allow_read_errors_with_report=True,
    )

    # Now, do the counting.
    logging.info(
        f"Generating the dask compute graph for {len(data.fields)} fields"  # type: ignore
    )
    total_count = sum(ak.count_nonzero(data[field]) for field in data.fields)  # type: ignore
    logging.info("Computing the total count")
    if dask_report:
        with performance_report(filename="dask-report.html"):
            r = total_count.compute()  # type: ignore
    else:
        r = total_count.compute()  # type: ignore

    logging.info(f"Done: result = {r:,}")

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
    format = "%(levelname)s - %(message)s"
    # format = "%(elapsed)s - %(levelname)s - %(message)s"
    if args.verbose == 1:
        root_logger.setLevel(level=logging.INFO)
    elif args.verbose >= 2:
        root_logger.setLevel(level=logging.DEBUG)
    else:
        root_logger.setLevel(level=logging.WARNING)
    root_logger.addHandler(handler)

    # Create the client dask worker
    if args.distributed_client == "local":
        # Do not know how to do it otherwise.
        n_workers = 8
        logging.debug("Creating local Dask cluster for {n_workers} workers")
        cluster = LocalCluster(
            n_workers=n_workers, processes=False, threads_per_worker=1
        )
        client = Client(cluster)
    elif args.distributed_client == "scheduler":
        logging.debug("Connecting to Dask scheduler at {scheduler_address}")
        assert args.dask_scheduler is not None
        client = Client(args.dask_scheduler)

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

    # Now run the main function
    if args.profile is False:
        main(
            ignore_cache=args.ignore_cache,
            num_files=args.num_files,
            dask_report=args.dask_profile,
            ds_name=ds_name,
            download_sx_result=args.download_sx_result,
        )
    else:
        cProfile.run(
            "main(ignore_cache=args.ignore_cache, num_files=args.num_files, "
            "dask_report=args.dask_profile, ds_name = ds_name, "
            "download_sx_result=args.download_sx_result)",
            "sx_materialize_branches.pstats",
        )
        logging.info("Profiling data saved to `sx_materialize_branches.pstats`")
