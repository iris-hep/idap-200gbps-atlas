# Input file list handling

Setting up everything for benchmarking purposes:

- `find_containers.py` queries rucio for a list of hardcoded DSIDs to (manually) build up `container_list.txt`
- `produce_container_metadata.py` takes `container_list.txt` and creates `container_metadata.json` for benchmarking calculations
- `get_file_list.py` takes a container and creates a list of file paths through UChicago XCaches
    - `containers_to_files.py` runs this for all containers in `container_list.txt` and creates a zipped version of the file lists `file_lists.zip`

Other utilities:

- `utils.py` is used to create filesets for benchmarking via its `get_fileset` function
