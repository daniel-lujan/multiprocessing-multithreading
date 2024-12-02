import re
import warnings
from os import listdir
from os.path import isfile, join
from random import randint
from threading import Thread
from time import time

import numpy as np
import pandas as pd

from bcolors import bcolors as b

warnings.filterwarnings("ignore")

data_directory = "./data/"
RANDOM_TESTS = 8

file_sizes = set()

for file in listdir(data_directory):
    if isfile(join(data_directory, file)):
        file_sizes.add(
            int(
                re.search(r"_\d+.csv", file)
                .group(0)
                .replace(".csv", "")
                .replace("_", "")
            )
        )

file_sizes = sorted(list(file_sizes))
print(f"{b.OKBLUE}File sizes found: {file_sizes}{b.ENDC}")


def get_random_line_from_file(file_name: str):
    with open(file_name, "r") as file:
        # file_name is formatted as 'file_{file_number}_{number_of_lines}.txt'
        _, file_number, lines = file_name.split("_")
        lines = int(lines.split(".")[0])

        random_line = randint(0, lines - 1)

        for i, line in enumerate(file):
            if i == random_line:
                return line, file_number, lines


def generate_target_values() -> pd.DataFrame:
    target_values = pd.DataFrame.from_dict(
        {"File Group": file_sizes, "Value": [None] * len(file_sizes)}
    )

    data_files = listdir(data_directory)

    for size in target_values["File Group"]:
        current_files = [file for file in data_files if f"_{size}.csv" in file]

        random_file = current_files[randint(0, len(current_files) - 1)]
        id_, file_number, lines = get_random_line_from_file(
            join(data_directory, random_file)
        )

        target_values.loc[target_values["File Group"] == size, "Value"] = id_.strip()

    return target_values


def search_for_value(file_name, value, results):
    start = time()
    with open(file_name, "r") as file:
        for i, line in enumerate(file):
            if results["found"]:
                # print("Early abort. Value found in another worker.")
                break
            if line.strip() == value:
                # print(
                #     f"{b.OKGREEN}Finishing worker. Value found in {file_name} at line {i + 1}{b.ENDC}"
                # )
                results["time"] = time() - start
                results["found"] = True
                break


def run_search_parallel(value: str, files: list[str]):
    threads = []
    results = {
        "time": -1,
        "found": False,
    }

    for file in files:
        thread = Thread(target=search_for_value, args=(file, value, results))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return results


def run_tests():
    results = pd.DataFrame(columns=["File Count", "Size", "Avg. Time"])

    for _ in range(RANDOM_TESTS):
        print(f"{b.OKGREEN}Test {_ + 1}/{RANDOM_TESTS}", end="")
        target_values = generate_target_values()
        test_results = pd.DataFrame(columns=["File Count", "Size", "Avg. Time"])

        for size in file_sizes:
            target = target_values[target_values["File Group"] == size]["Value"].values[
                0
            ]
            files = [
                join(data_directory, f)
                for f in listdir(data_directory)
                if f"_{size}.csv" in f
            ]
            start_time = time()
            size_result = run_search_parallel(
                target,
                files,
            )

            end_time = time() - start_time

            test_results = pd.concat(
                [
                    test_results,
                    pd.DataFrame(
                        [[len(files), size, end_time]],
                        columns=["File Count", "Size", "Avg. Time"],
                    ),
                ],
                ignore_index=True,
            )

        results = (
            pd.concat([results, test_results], ignore_index=True)
            .groupby("Size")
            .mean()
            .reset_index()
        )

        print(f" - COMPLETED{b.ENDC}")

    return results


def main():
    print(f"{b.HEADER}Ejecutando test con Multithreading...{b.ENDC}")
    results = run_tests()
    results.to_csv("results-multithreading.csv", index=False)
    print(
        f"Test finalizado. Resultados guardados en {b.UNDERLINE}'results-multithreading.csv'{b.ENDC}"
    )


if __name__ == "__main__":
    main()
