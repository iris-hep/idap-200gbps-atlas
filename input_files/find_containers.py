import os

container_dict = {
    "db": [
        345705,
        345706,
        345723,
        363355,
        363356,
        363357,
        363358,
        363359,
        363360,
        363489,
        363494,
        364250,
        364254,
        364255,
        364283,
        364253,
        364284,
        364285,
        364287,
        364288,
        364289,
        364290,
    ],
    "zjets": [
        364100,
        364101,
        364102,
        364103,
        364104,
        364105,
        364106,
        364107,
        364108,
        364109,
        364110,
        364111,
        364112,
        364113,
        364114,
        364115,
        364116,
        364117,
        364118,
        364119,
        364120,
        364121,
        364122,
        364123,
        364124,
        364125,
        364126,
        364127,
        364128,
        364129,
        364130,
        364131,
        364132,
        364133,
        364134,
        364135,
        364136,
        364137,
        364138,
        364139,
        364140,
        364141,
        364198,
        364199,
        364200,
        364201,
        364202,
        364203,
        364204,
        364205,
        364206,
        364207,
        364208,
        364209,
        364210,
        364211,
        364212,
        364213,
        364214,
        364215,
    ],
    "wjets": [
        364156,
        364157,
        364158,
        364159,
        364160,
        364161,
        364162,
        364163,
        364164,
        364165,
        364166,
        364167,
        364168,
        364169,
        364170,
        364171,
        364172,
        364173,
        364174,
        364175,
        364176,
        364177,
        364178,
        364179,
        364180,
        364181,
        364182,
        364183,
        364184,
        364185,
        364186,
        364187,
        364188,
        364189,
        364190,
        364191,
        364192,
        364193,
        364194,
        364195,
        364196,
        364197,
    ],
    "ttV": [
        410155,
        410156,
        410157,
        410218,
        410219,
        410220,
        410276,
        410277,
        410278,
    ],
    "othertop": [
        410644,
        410645,
        410658,
        410659,
        601352,
        601355,
        410560,
        410408,
        412043,
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
