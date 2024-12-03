import threading
import time
import warnings

import psutil

warnings.filterwarnings("ignore")

from os import makedirs, path, remove

import test_multiprocessing
import test_multithreading
from bcolors import bcolors as b
from common import RANDOM_TESTS, generate_target_values, target_values_dir


def monitor_resources(process, interval, stop_event, usage_data):
    while not stop_event.is_set():
        cpu_percent = process.cpu_percent(interval=None)
        memory_info = process.memory_info()

        usage_data["cpu"].append(cpu_percent)
        usage_data["memory"].append(memory_info.rss / (1024 * 1024))

        time.sleep(interval)


def track_usage(func, description):
    process = psutil.Process()
    usage_data = {"cpu": [], "memory": []}
    stop_event = threading.Event()
    monitor_thread = threading.Thread(
        target=monitor_resources,
        args=(process, 0.5, stop_event, usage_data),
    )

    start_time = time.perf_counter()
    print(f"\n{b.OKBLUE}Starting {description}{b.ENDC}")

    monitor_thread.start()

    func()
    stop_event.set()
    monitor_thread.join()

    end_time = time.perf_counter()

    peak_cpu = max(usage_data["cpu"]) if usage_data["cpu"] else 0
    peak_memory = max(usage_data["memory"]) if usage_data["memory"] else 0

    print(f"{b.OKGREEN}{description} completed!{b.ENDC}")
    print(f"Peak CPU usage: {peak_cpu:.2f}%")
    print(f"Peak Memory usage: {peak_memory:.2f} MB")
    print(f"Elapsed time: {end_time - start_time:.2f} seconds\n")


if __name__ == "__main__":
    if not path.exists(target_values_dir):
        makedirs(target_values_dir)

    for i in range(RANDOM_TESTS):
        values = generate_target_values()
        values.to_csv(target_values_dir + f"target_values_{i}.csv", index=False)

    print(
        f"{b.OKGREEN}Target values generated and saved in {target_values_dir}{b.ENDC}"
    )

    track_usage(test_multithreading.main, "Multithreading")
    track_usage(test_multiprocessing.main, "Multiprocessing")

    for i in range(RANDOM_TESTS):
        remove(target_values_dir + f"target_values_{i}.csv")
