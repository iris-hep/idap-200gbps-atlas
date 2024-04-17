"""
This code takes as input a dataset name and outputs a file with modified paths that use UC AF XCaches.
XCaches are hardcoded.

Simply do:
setupATLAS
lsetup rucio
voms-proxy-init
pip install rucio-clients xmltodict
python3 get_file_list.py usr.adohnalo:user.adohnalo.410470.PhPy8EG.DAOD_PHYS.e6337_s3681_r13145_p5980.JES_16feb_v6_output
"""
import sys
import logging
import xmltodict
import hashlib
import os

from rucio.common.exception import DataIdentifierNotFound
from rucio.client.scopeclient import ScopeClient
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient

logging.basicConfig()
logging.root.setLevel(logging.INFO)

logger = logging.getLogger("file_list")
logger.info("DID Finder starting up.")
did_client = DIDClient()
replica_client = ReplicaClient()

caches = ['192.170.240.141', '192.170.240.142', '192.170.240.143',
          '192.170.240.144', '192.170.240.145', '192.170.240.146',
          '192.170.240.147', '192.170.240.148']

if len(sys.argv) > 1:
    did = sys.argv[1]
else:
    print("Please provide a dataset name.")
    sys.exit(1)


def parse_did(did):
    """
    Parse a DID string into the scope and name
    Allow for no scope to be included
    :param did:
    :return: Dictionary with keys "scope" and "name"
    """
    d = dict()
    if ':' in did:
        d['scope'], d['name'] = did.split(":")
        return d

    all_scopes = ScopeClient().list_scopes()

    for sc in all_scopes:
        if did.startswith(sc):
            d['scope'], d['name'] = sc, did
            return d

    logger.error(f"Scope of the dataset {did} could not be determined.")
    return None


parsed_did = parse_did(did)
if not parsed_did:
    sys.exit(2)


datasets = []
try:
    did_info = did_client.get_did(parsed_did['scope'], parsed_did['name'])
    if did_info['type'] == 'CONTAINER':
        logger.info(f"{did} is a container of {did_info['length']} datasets.")
        content = did_client.list_content(parsed_did['scope'], parsed_did['name'])
        for c in content:
            datasets.append([c['scope'], c['name']])
    elif did_info['type'] == 'DATASET':
        datasets.append([parsed_did['scope'], parsed_did['name']])
        logger.info(f"{did} is a dataset with {did_info['length']} files.")
    else:
        logger.info(f"{did} is a file: {did_info}.")
        datasets.append([parsed_did['scope'], parsed_did['name']])
except DataIdentifierNotFound:
    logger.warning(f"{did} not found")
    sys.exit(3)

files = []
total_size = 0
no_replica_files = 0

for ds in datasets:
    reps = replica_client.list_replicas(
        [{'scope': ds[0], 'name': ds[1]}],
        schemes=['root', 'http', 'https'],
        metalink=True,
        sort='geoip',
        rse_expression='MWT2_UC_LOCALGROUPDISK|MWT2_UC_SCRATCHDISK|MWT2_DATADISK',
        ignore_availability=False
    )
    d = xmltodict.parse(reps)
    if 'file' in d['metalink']:
        # if only one file, xml returns a dict and not a list.
        if isinstance(d['metalink']['file'], dict):
            mfile = [d['metalink']['file']]
        else:
            mfile = d['metalink']['file']
        for f in mfile:
            # Path is either a list of replicas or a single logical name
            if 'url' not in f:
                logger.error(f"File {f['identity']} has no replicas.")
                no_replica_files += 1
                continue

            if isinstance(f['url'], dict):
                path = f['url']['#text']
            else:
                path = f['url'][0]['#text']

            total_size += int(f['size'], 10)
            files.append(path)

logger.info(
    f"{did} is a dataset with {len(files)} files. Total dataset size is {total_size/1024/1024/1024} GB")

for f in files:
    if f.count("root://") > 1:
        f = f[f.index("root://", 2):]


def hash_string(input_string):
    # Create a SHA-256 hash object
    hash_object = hashlib.sha256(input_string.encode())

    # Get the hexadecimal digest
    hex_digest = hash_object.hexdigest()

    # Convert the hexadecimal digest to an integer
    int_value = int(hex_digest, 16)

    return int_value


output_directory = "file_lists"
if not os.path.exists(output_directory):
    os.mkdir(output_directory)

cf = []
for f in files:
    c = hash_string(f) % len(caches)
    cf.append(f'root://{caches[c]}//{f}')
    print(f)

with open(f'{output_directory}/{did.replace(":", "-")}.txt', 'w') as file:
    for f in cf:
        file.write(f + '\n')
