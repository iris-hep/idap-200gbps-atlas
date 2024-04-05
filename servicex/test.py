import uproot
import awkward as ak
import multiprocessing
from dask.distributed import Client, LocalCluster


n_workers = multiprocessing.cpu_count()
print(n_workers)
n_workers = 2
cluster = LocalCluster(
    n_workers=n_workers, processes=False, threads_per_worker=1
)
client = Client(cluster)

data = uproot.dask({
    "/data/gwatts/test.root": "atlas_xaod_tree"
})

total = ak.count(data.jet_pt)

print(total.compute())
