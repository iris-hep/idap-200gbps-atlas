# process list of containers into list of files with hardcoded xcache instances

# to run get_file_list.py, use e.g. a venv on uchicago via ssh
# python3 -m venv venv
# source venv/bin/activate
# pip install xmltodict
# (assuming setupATLAS / lsetup rucio + proxy present)

import os
import shutil

if __name__ == "__main__":
    with open("container_list.txt") as f:
        containers = f.readlines()

    for container in containers:
        container = container.strip()

        if "#" in container:
            continue  # skip comments

        cmd = f"python get_file_list.py {container}"
        print(cmd)
        os.system(cmd)  # produce file list

    # create zipped version of folder with file lists
    shutil.make_archive("file_lists", "zip", "file_lists")

    # cleanup: delete non-zipped version
    shutil.rmtree("file_lists")
