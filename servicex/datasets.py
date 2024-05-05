from typing import List


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
    else:
        raise RuntimeError(f"Unknown dataset option: {ds_option}")
