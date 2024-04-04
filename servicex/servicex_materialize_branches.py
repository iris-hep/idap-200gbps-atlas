import logging
from typing import List

import awkward as ak
import uproot
from func_adl_servicex_xaodr21 import SXDSAtlasxAODR21, atlas_release

from servicex import ServiceXDataset

# TODO: Update to use R22/23 or whatever.


def query_servicex() -> List[str]:
    """Load and execute the servicex query. Returns a complete list of paths
    (be they local or url's) for the root or parquet files.
    """
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
        })
    )
    # fmt: on

    # Do the query.
    ds_prime = ServiceXDataset(rucio_ds, backend_name="atlasr22")
    files = ds_prime.get_data_rootfiles(query.value(), title="First Request")

    return [str(f) for f in files]


def main():
    """Match the operations found in `materialize_branches` notebook:
    Load all the branches from some dataset, and then count the flattened
    number of items, and, finally, print them out.
    """
    logging.info(f"Using release {atlas_release}")

    # Execute the query and get back the files.
    files = query_servicex()

    # now materialize everything.
    data = uproot.dask({f: "atlas_xaod_tree" for f in files})
    total_count = (
        ak.count(data["event_number"])  # type: ignore
        + ak.count(ak.flatten(data["jet_pt"]))
        + ak.count(data["run_number"])
    )
    r = total_count.compute()
    print(f"{r:,}")


if __name__ == "__main__":

    main()
