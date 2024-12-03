import re
from os import listdir
from os.path import isfile, join
from random import randint
from time import perf_counter
from typing import Callable

import pandas as pd

from bcolors import bcolors as b

data_directory = "./data/"
target_values_dir = "./temp/target_values/"
RANDOM_TESTS = 40


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

if __name__ == "__main__":
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


def run_tests(run_search_parallel: Callable):
    results = pd.DataFrame(columns=["File Count", "Size", "Avg. Time"])

    for i in range(RANDOM_TESTS):
        print(f"{b.OKGREEN}Test {i + 1}/{RANDOM_TESTS}", end="")
        target_values = generate_target_values()
        test_results = pd.DataFrame(columns=["File Count", "Size", "Avg. Time"])

        for size in file_sizes:
            target = pd.read_csv(join(target_values_dir, f"target_values_{i}.csv"))[
                target_values["File Group"] == size
            ]["Value"].values[0]

            files = [
                join(data_directory, f)
                for f in listdir(data_directory)
                if f"_{size}.csv" in f
            ]
            start_time = perf_counter()
            size_result = run_search_parallel(
                target,
                files,
            )

            end_time = perf_counter() - start_time

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

        results = pd.concat([results, test_results], ignore_index=True)

        print(f" - COMPLETED{b.ENDC}")

    return results
