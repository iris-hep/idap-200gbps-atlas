from collections import defaultdict
import json
from pathlib import Path
import zipfile

from . import find_containers

DIR = Path(__file__).parent.resolve()


def get_dsids(process):
    if "data" not in process:
        return find_containers.container_dict[process]
    else:
        return [process]


def get_fileset(processes_to_use, max_files_per_container=None):
    with open(DIR / "container_metadata.json") as f:
        container_metadata = json.load(f)  # name -> metadata

    container_to_file_list = {}  # container name -> list of files

    # read from zipped file
    with zipfile.ZipFile(DIR / "file_lists.zip") as z:
        for filename in sorted(z.namelist()):
            container_name = filename.split("/")[-1:][0].replace("-", ":").replace(".txt", "")
            with z.open(filename) as f:
                file_list = f.readlines()

            # limit amount of files per container
            if max_files_per_container is not None:
                file_list = file_list[:max_files_per_container]

            container_to_file_list[container_name] = [p.decode("utf-8").strip() for p in file_list]

    fileset = defaultdict(lambda: defaultdict(dict))  # process -> list of files
    total_nfiles = 0
    total_size_TB = 0
    total_nevts = 0
    for process in processes_to_use:
        dsids = get_dsids(process)
        for dsid in dsids:
            # find matching containers
            matching_containers = [c for c in list(container_to_file_list.keys()) if str(dsid) in c]
            # for each container, add full list of files
            for container in matching_containers:
                file_list = container_to_file_list[container]
                total_nfiles += len(file_list)
                if max_files_per_container is None:
                    assert len(file_list) == container_metadata[container]["nfiles"]
                total_size_TB += container_metadata[container]["size_TB"]
                total_nevts += container_metadata[container]["nevts"]
                fileset[process]["files"].update(dict(zip(file_list, ["CollectionTree"]*len(file_list))))

    print("fileset summary")
    print(f" - number of files: {total_nfiles:,}")
    if max_files_per_container is None:
        print(f" - total size: {total_size_TB:.3f} TB")
        print(f" - number of nevts: {total_nevts:,}")
    else:
        print("cannot determine total size / number of events when max_files_per_container is being used")

    return fileset


if __name__ == "__main__":
    processes = ["db", "zjets", "wjets", "ttV", "othertop", "ttbar", "data15_13TeV", "data16_13TeV", "data17_13TeV", "data18_13TeV"]
    get_fileset(processes)
