import datetime
import json
import os
import pathlib
import threading
import time

import awkward as ak
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np


FNAME_OUT = "num_workers.txt"
FNAME_FLAG = "DASK_RUNNING"


def write_num_workers(fname, client, interval):
    with open(fname, "w") as f:
        while os.path.isfile("DASK_RUNNING"):
            # could also use client.nthreads
            f.write(f"{datetime.datetime.now()}, {len(client.scheduler_info()['workers'])}\n")
            f.flush()
            time.sleep(interval)


def start_tracking_workers(client, measurement_path, interval=1):
    pathlib.Path(FNAME_FLAG).touch()  # file that indicates worker tracking should run
    nworker_thread = threading.Thread(target=write_num_workers, args=(measurement_path / FNAME_OUT, client, interval))
    nworker_thread.start()  # track workers in background
    return nworker_thread


def stop_tracking_workers():
    pathlib.Path(FNAME_FLAG).unlink()
    print("sleeping for two seconds before proceeding to ensure tracking is done")
    time.sleep(2)


def get_timestamps_and_counts(measurement_path):
    with open(measurement_path / FNAME_OUT) as f:
        content = [l.strip().split(", ") for l in f.readlines()]

    timestamps = [datetime.datetime.strptime(c[0],  '%Y-%m-%d %H:%M:%S.%f') for c in content]
    nworkers = [int(c[1]) for c in content]

    delta_t = [(timestamps[i+1] - timestamps[i]).seconds for i in range(len(timestamps)-1)]
    workers_times_time = [nworkers[:-1][i] * delta_t[i] for i in range(len(timestamps)-1)]
    avg_num_workers = sum(workers_times_time) / (timestamps[-1] - timestamps[0]).seconds

    return timestamps, nworkers, avg_num_workers


def save_measurement(out, t0, t1, measurement_path):
    # save results to json
    with open(measurement_path / "out.json", "w") as f:
        f.write(ak.to_json(out, convert_other=str))

    with open(measurement_path / "start_end_time.txt", "w") as f:
        f.write(f"{t0},{t1}")


def load_measurement(measurement_path):
    # load results from json (datetime becomes str)
    with open(measurement_path / "out.json") as f:
        content = f.readlines()

    out = ak.from_json(content[0])
    out["time_finished"] = ak.Array(np.asarray(out["time_finished"], dtype=np.datetime64))  # convert back to np.datetime64

    with open(measurement_path / "start_end_time.txt") as f:
        t0, t1 = [float(t) for t in f.readlines()[0].split(",")]

    return out, t0, t1


def save_fileset(fileset, measurement_path):
    fileset = dict((k, dict(v.items())) for k, v in fileset.items())  # defaultdict to dict
    with open(measurement_path / "fileset.json", "w") as f:
        json.dump(fileset, f, sort_keys=True, indent=4)


def plot_worker_count(timestamps, nworkers, avg_num_workers, times_for_rates, instantaneous_rates, measurement_path):
    fig, ax1 = plt.subplots(constrained_layout=True)
    ax1.plot(timestamps, nworkers, linewidth=2, color="C0")
    ax1.set_xlabel("time")
    ax1.set_ylabel("number of workers", color="C0")
    ax1.set_ylim([0, ax1.get_ylim()[1]*1.1])
    # ax1.set_title(f"worker count over time, average: {avg_num_workers:.1f}")
    ax1.set_title(f"worker count and data rate over time")
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax1.tick_params(axis="y", labelcolor="C0")

    for tick in ax1.get_xticklabels():
        tick.set_rotation(45)
        tick.set_horizontalalignment("right")

    ax2 = ax1.twinx()
    ax2.plot(times_for_rates, instantaneous_rates, marker="v", linewidth=0, color="C1")
    ax2.set_ylabel("data rate [Gbps]", color="C1")
    ax2.set_ylim([0, ax2.get_ylim()[1]*1.1])
    ax2.tick_params(axis="y", labelcolor="C1")
    fig.savefig(measurement_path / "worker_count_data_rate_over_time.pdf")