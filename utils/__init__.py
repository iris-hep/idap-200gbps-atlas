import datetime
import threading
import time
import pathlib
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


FNAME_OUT = "num_workers.txt"
FNAME_FLAG = "DASK_RUNNING"


def write_num_workers(fname, client, interval):
    with open(fname, "w") as f:
        while os.path.isfile("DASK_RUNNING"):
            # could also use client.nthreads
            f.write(f"{datetime.datetime.now()}, {len(client.scheduler_info()['workers'])}\n")
            f.flush()
            time.sleep(interval)


def start_tracking_workers(client, interval=1):
    pathlib.Path('DASK_RUNNING').touch()  # file that indicates worker tracking should run
    nworker_thread = threading.Thread(target=write_num_workers, args=("num_workers.txt", client, interval))
    nworker_thread.start()  # track workers in background
    return nworker_thread


def stop_tracking_workers():
    pathlib.Path('DASK_RUNNING').unlink()


def get_timestamps_and_counts():
    print("sleeping for two seconds before reading out file to ensure tracking is done")
    time.sleep(2)
    with open("num_workers.txt") as f:
        content = [l.strip().split(", ") for l in f.readlines()]

    timestamps = [datetime.datetime.strptime(c[0],  '%Y-%m-%d %H:%M:%S.%f') for c in content]
    nworkers = [int(c[1]) for c in content]

    delta_t = [(timestamps[i+1] - timestamps[i]).seconds for i in range(len(timestamps)-1)]
    workers_times_time = [nworkers[:-1][i] * delta_t[i] for i in range(len(timestamps)-1)]
    avg_num_workers = sum(workers_times_time) / (timestamps[-1] - timestamps[0]).seconds

    return timestamps, nworkers, avg_num_workers


def plot_worker_count(timestamps, nworkers, avg_num_workers):
    fig, ax = plt.subplots()
    ax.plot(timestamps, nworkers)
    ax.set_xlabel("time")
    ax.set_ylabel("number of workers")
    ax.set_ylim([0, ax.get_ylim()[1]*1.2])
    ax.set_title(f"worker count over time, average: {avg_num_workers:.1f}")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))

    for tick in ax.get_xticklabels():
        tick.set_rotation(45)
        tick.set_horizontalalignment("right")
