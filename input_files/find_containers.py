import os

container_dict = {
    "db": [
        700587,  # VV EWK
        700588,
        700589,
        700590,
        700591,
        700592,
        700593,
        700594,
        700600,  # VV QCD
        700601,
        700602,
        700603,
        700604,
        700605,
    ],
    "zjets": [
        700320,  # Zee
        700321,
        700322,
        700323,  # Zmumu
        700324,
        700325,
        700335,  # Znunu
        700336,
        700337,
        700792,  # Ztautau
        700793,
        700794,
    ],
    "wjets": [
        700338,  # Wenu
        700339,
        700340,
        700341,  # Wmunu
        700342,
        700343,
        700344,  # Wtaunu
        700345,
        700346,
        700347,
        700348,
        700349,
    ],
    "ttV": [
        410155,  # ttW
        504338,  # ttZ
        504346,
        504330,  # ttll
        504334,
        504342,
    ],
    "othertop": [
        410644,  # s-chan
        410645,
        410658,  # t-chan
        410659,
        601352,  # tW
        601355,
        410560,  # tZ, old sample
        525955,  # tWZ, exists in fastsim
        412043,  # 4top, exists in fastsim
    ],
    "ttbar": [410470],
}


### helper to check for missing containers
def find_missing_containers():
    with open("container_list.txt") as f:
        containers = f.readlines()

    all_dsids = set()
    for cc in container_dict.values():
        all_dsids |= set(cc)

    dsids_found = set()
    for cc in containers:
        if cc.startswith("#"):
            continue

        try:
            dsid = int(cc.split(".")[1])
        except:
            assert "AllYear" in cc.split(".")[1]
        dsids_found.add(dsid)

    print(f"missing containers for {set(all_dsids) - dsids_found}")


# r-tag info: https://twiki.cern.ch/twiki/bin/view/AtlasProtected/AtlasProductionGroupMC20
# mc20a -> r13167
# mc20d -> r13144
# mc20e -> r13145
# r13146 -> AOD merge
# AFII: r14859 / r14860 / r14861


def query_rucio(container_dict):
    """Find containers with rucio for all desired DSIDs."""
    for k, v in container_dict.items():
        print(f"# {k}")
        for dsid in v:
            cmd = f"rucio list-dids --filter 'type=CONTAINER' --short mc20_13TeV:mc20_13TeV.{dsid}*PHYSLITE* | grep -E 'r13167|r13144|r13145' | grep -v r13146 | grep p6026"
            print(cmd)
            os.system(cmd)


if __name__ == "__main__":
    query_rucio(container_dict)
    find_missing_containers()
