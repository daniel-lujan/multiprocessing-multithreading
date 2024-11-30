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
RANDOM_TESTS = 2


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
    target_values = pd.DataFrame(
        columns=["File Group", "Lines", "Value", "Source Filename"]
    )

    processed_groups = set()

    for filename in listdir(data_directory):
        if isfile(join(data_directory, filename)):
            match = re.match(r"file_(\d+)_\d+\.csv", filename)
            if match:
                file_group = match.group(1)

                if file_group not in processed_groups:
                    id_, file_number, lines = get_random_line_from_file(
                        join(data_directory, filename)
                    )
                    id_ = id_.strip()
                    target_values = pd.concat(
                        [
                            target_values,
                            pd.DataFrame(
                                {
                                    "File Group": file_group,
                                    "Lines": lines,
                                    "Value": id_,
                                    "Source Filename": join(data_directory, filename),
                                },
                                index=[0],
                            ),
                        ]
                    )
                    processed_groups.add(file_group)

    return target_values


def search_for_value(file_name, value, results):
    start = time()
    with open(file_name, "r") as file:
        for i, line in enumerate(file):
            if line.strip() == value:
                test_time = time() - start
                results.append(test_time)


def run_search_parallel(targets: pd.DataFrame):
    threads = []
    results = []

    for args in targets.values:
        thread = Thread(target=search_for_value, args=(args[3], args[2], results))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return results


def run_tests():
    results = pd.DataFrame(columns=["Size", "Avg. Time"])

    for _ in range(RANDOM_TESTS):
        print(f"{b.OKGREEN}Test {_ + 1}/{RANDOM_TESTS}", end="")
        target_values = generate_target_values()
        sizes = target_values["Lines"].unique()
        test_results = pd.DataFrame(columns=["Size", "Avg. Time"])

        for size in sizes:
            targets = target_values.where(target_values["Lines"] == size).dropna()
            size_result = run_search_parallel(targets)

            test_results = pd.concat(
                [
                    test_results,
                    pd.DataFrame(
                        [[size, np.mean(size_result)]], columns=["Size", "Avg. Time"]
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
