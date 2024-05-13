from pathlib import Path
from typing import Dict, List

from pydantic import BaseModel, RootModel
import json


class Data(BaseModel):
    """Data class for single sample."""

    nevts: int
    nfiles: int
    size_TB: float


class ContainerMetadata(RootModel):
    """Container metadata class."""

    root: Dict[str, Data]


def load_containers() -> Dict[str, Data]:
    """Load the container metadata.json file.

    Returns:
        Dict[str, Dict[str, AnyStr]]: Container Metadata for the text files.
    """
    metadata_path = (
        Path(__file__).parent.parent / "input_files" / "container_metadata.json"
    )
    with metadata_path.open("r") as f:
        metadata = json.load(f)
    return ContainerMetadata.model_validate(metadata).root


def determine_dataset(ds_option: str) -> List[str]:
    """Return a list of datasets that we will use for testing,
    depending on the option given on the command line.

    Args:
        ds_option (str): Option from the command line

    Returns:
        List[str]: List of datasets.
    """
    if ds_option == "mc_10TB":
        return [
            "data15_13TeV.periodAllYear.physics_Main.PhysCont.DAOD_PHYSLITE.grp15_v01_p6026"
        ]
    elif ds_option == "data_50TB":
        return [
            "data18_13TeV.periodAllYear.physics_Main.PhysCont.DAOD_PHYSLITE.grp18_v01_p6026"
        ]
    elif ds_option == "mc_1TB":
        return [
            "mc20_13TeV.364157.Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV0_70_CFilterBVeto.deriv"
            ".DAOD_PHYSLITE.e5340_s3681_r13145_p6026"
        ]
    elif ds_option == "multi_1TB":
        all = load_containers()
        return [k for k, v in all.items() if v.size_TB >= 1.0 and v.size_TB < 2.0]
    elif ds_option == "multi_small_10":
        all = load_containers()
        return [k for k, v in all.items() if v.size_TB < 1.0][0:10]
    elif ds_option == "multi_small_20":
        all = load_containers()
        return [k for k, v in all.items() if v.size_TB < 1.0][0:20]
    elif ds_option == "multi_data":
        all = load_containers()
        return [k for k, v in all.items() if "data" in str(k)]
    elif ds_option == "all":
        all = load_containers()
        return list(all.keys())
    else:
        raise RuntimeError(f"Unknown dataset option: {ds_option}")
