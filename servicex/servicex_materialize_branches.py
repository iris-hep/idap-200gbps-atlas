import argparse
import cProfile
import logging
import multiprocessing
import time
from typing import List

import awkward as ak
import uproot
from dask.distributed import Client, LocalCluster
from func_adl_servicex_xaodr21 import SXDSAtlasxAODR21, atlas_release

from servicex import ServiceXDataset

# TODO: Update to use R22/23 or whatever.


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


def query_servicex(ignore_cache: bool) -> List[str]:
    """Load and execute the servicex query. Returns a complete list of paths
    (be they local or url's) for the root or parquet files.
    """
    logging.info("Building ServiceX query")
    ds_name = (
        "mc23_13p6TeV.601229.PhPy8EG_A14_ttbar_hdamp258p75_SingleLep"
        ".deriv.DAOD_PHYSLITE.e8514_s4162_r14622_p6026"
    )

    # Build the data query for SX
    rucio_ds = f"rucio://{ds_name}?files=4"

    # Because we are going to do a specialized query, we'll alter the return type here.
    ds = SXDSAtlasxAODR21(rucio_ds, backend="atlasr22")
    ds.return_qastle = True

    # Build the query
    # TODO: The EventInfo argument should default correctly
    #       (that may just be a matter of using func_adl xaod r22)
    # TODO: dataclass should be supported so as not to lose type-following!
    # TODO: once https://github.com/iris-hep/func_adl/issues/136 fixed, turn off fmt off.
    # fmt: off
    query = (ds.Select(lambda e: {
            "evt": e.EventInfo("EventInfo"),
            "jet": e.Jets("AnalysisJets", calibrate=False),
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
                    lambda j: j.getAttributeVectorFloat("EnergyPerSampling")
                ),
            "jet_SumPtTrkPt500":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttributeVectorFloat("SumPtTrkPt500")
                ),
            "jet_TrackWidthPt1000":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttributeVectorFloat("TrackWidthPt1000")
                ),
            # "jet_NumTrkPt500":
            #     ei.jet.Select(  # type: ignore
            #         lambda j: j.getAttributeVectorInt("NumTrkPt500")
            #     ),
            # "jet_NumTrkPt1000":
            #     ei.jet.Select(  # type: ignore
            #         lambda j: j.getAttributeVectorInt("NumTrkPt1000")
            #     ),
        })
    )

    # _counter += ak.count_nonzero(events.Jets.SumPtChargedPFOPt500)
    # _counter += ak.count_nonzero(events.Jets.Timing)
    # _counter += ak.count_nonzero(events.Jets.JetConstitScaleMomentum_eta)
    # _counter += ak.count_nonzero(events.Jets.ActiveArea4vec_eta)
    # _counter += ak.count_nonzero(events.Jets.DetectorEta)
    # _counter += ak.count_nonzero(events.Jets.JetConstitScaleMomentum_phi)
    # _counter += ak.count_nonzero(events.Jets.ActiveArea4vec_phi)
    # _counter += ak.count_nonzero(events.Jets.JetConstitScaleMomentum_m)
    # _counter += ak.count_nonzero(events.Jets.JetConstitScaleMomentum_pt)
    # _counter += ak.count_nonzero(events.Jets.Width)
    # _counter += ak.count_nonzero(events.Jets.EMFrac)
    # _counter += ak.count_nonzero(events.Jets.ActiveArea4vec_m)
    # _counter += ak.count_nonzero(events.Jets.ActiveArea4vec_pt)
    # _counter += ak.count_nonzero(events.Jets.DFCommonJets_QGTagger_TracksWidth)
    # _counter += ak.count_nonzero(events.Jets.JVFCorr)
    # _counter += ak.count_nonzero(events.Jets.DFCommonJets_QGTagger_TracksC1)
    # _counter += ak.count_nonzero(events.Jets.PSFrac)
    # _counter += ak.count_nonzero(events.Jets.DFCommonJets_QGTagger_NTracks)
    # _counter += ak.count_nonzero(events.Jets.DFCommonJets_fJvt)
    # _counter += ak.count_nonzero(events.Jets.PartonTruthLabelID)
    # _counter += ak.count_nonzero(events.Jets.HadronConeExclExtendedTruthLabelID)
    # _counter += ak.count_nonzero(events.Jets.ConeTruthLabelID)
    # _counter += ak.count_nonzero(events.Jets.HadronConeExclTruthLabelID)

    # fmt: on

    # Do the query.
    ds_prime = ServiceXDataset(
        rucio_ds, backend_name="atlasr22", ignore_cache=ignore_cache
    )
    logging.info("Starting ServiceX query")
    files = ds_prime.get_data_rootfiles_uri(query.value(), title="First Request", as_signed_url=True)
    logging.info("Finished ServiceX query")

    return [str(f.url) for f in files]


def main(ignore_cache: bool = False):
    """Match the operations found in `materialize_branches` notebook:
    Load all the branches from some dataset, and then count the flattened
    number of items, and, finally, print them out.
    """
    logging.info(f"Using release {atlas_release}")

    # Execute the query and get back the files.
    files = query_servicex(ignore_cache=ignore_cache)

    # now materialize everything.
    logging.info("Using `uproot.dask` to open files")
    data = uproot.dask({f: "atlas_xaod_tree" for f in files})
    logging.info(
        f"Generating the dask compute graph for {len(data.fields)} fields"  # type: ignore
    )
    total_count = sum(ak.count_nonzero(data[field]) for field in data.fields)  # type: ignore
    logging.info("Computing the total count")
    r = total_count.compute()
    logging.info(f"Done: result = {r:,}")


if __name__ == "__main__":
    # This block of code just setups for the run (command line arguments, logging, etc)

    # Create the argument parser
    parser = argparse.ArgumentParser(
        description="Run simple ServiceX query and processing"
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

    # Add the flag to enable/disable local Dask cluster
    parser.add_argument(
        "--distributed-client",
        choices=["local", "none"],
        default="local",
        help="Specify the type of Dask cluster to enable (default: local uses all cores "
        "in process, none doesn't use any)",
    )

    # Parse the command line arguments
    args = parser.parse_args()

    # Create the client dask worker
    if args.distributed_client == "local":
        n_workers = multiprocessing.cpu_count()
        cluster = LocalCluster(
            n_workers=n_workers, processes=False, threads_per_worker=1
        )
        client = Client(cluster)

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

    # Now run the main function
    if args.profile is False:
        main(ignore_cache=args.ignore_cache)
    else:
        cProfile.run(
            "main(ignore_cache=args.ignore_cache)", "sx_materialize_branches.pstats"
        )
        logging.info("Profiling data saved to `sx_materialize_branches.pstats`")
