from dataclasses import dataclass
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


@dataclass
class data_info:
    """Info for a run - list of samples, total size, total events, etc."""

    samples: List[str]
    total_size_TB: float
    total_events: int


def make_data_info(ds_list: List[str], all: Dict[str, Data]) -> data_info:
    """Get the data info for the given list of datasets.

    Args:
        ds_list (List[str]): List of datasets
        all (Dict[str, Dict[str, AnyStr]]): All the datasets

    Returns:
        data_info: Info for a run - list of samples, total size, total events, etc.
    """
    total_size_TB = 0.0
    total_events = 0
    for ds in ds_list:
        total_size_TB += all[ds].size_TB
        total_events += all[ds].nevts
    return data_info(ds_list, total_size_TB, total_events)


def determine_dataset(ds_option: str) -> data_info:
    """Return a list of datasets that we will use for testing,
    depending on the option given on the command line.

    Args:
        ds_option (str): Option from the command line

    Returns:
        List[str]: List of datasets.
    """
    all = load_containers()
    if ds_option == "mc_10TB":
        ds_list = [
            "data15_13TeV:data15_13TeV.periodAllYear.physics_Main.PhysCont.DAOD_PHYSLITE"
            ".grp15_v01_p6026"
        ]
        return make_data_info(ds_list, all)
    elif ds_option == "data_50TB":
        return make_data_info(
            [
                "data18_13TeV:data18_13TeV.periodAllYear.physics_Main.PhysCont.DAOD_PHYSLITE"
                ".grp18_v01_p6026"
            ],
            all,
        )
    elif ds_option == "mc_1TB":
        return make_data_info(
            [
                "mc20_13TeV:mc20_13TeV.364157."
                "Sherpa_221_NNPDF30NNLO_Wmunu_MAXHTPTV0_70_CFilterBVeto.deriv"
                ".DAOD_PHYSLITE.e5340_s3681_r13145_p6026"
            ],
            all,
        )
    elif ds_option == "multi_1TB":
        return make_data_info(
            [k for k, v in all.items() if v.size_TB >= 1.0 and v.size_TB < 2.0], all
        )
    elif ds_option == "multi_small_10":
        return make_data_info([k for k, v in all.items() if v.size_TB < 1.0][0:10], all)
    elif ds_option == "multi_small_20":
        return make_data_info([k for k, v in all.items() if v.size_TB < 1.0][0:20], all)
    elif ds_option == "multi_data":
        return make_data_info([k for k, v in all.items() if "data" in str(k)], all)
    elif ds_option == "all":
        return make_data_info(list(all.keys()), all)
    else:
        raise RuntimeError(f"Unknown dataset option: {ds_option}")
