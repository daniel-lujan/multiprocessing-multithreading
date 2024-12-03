from threading import Thread
from time import time

from bcolors import bcolors as b
from common import run_tests


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


def main():
    print(f"{b.HEADER}Ejecutando test con Multithreading...{b.ENDC}")
    results = run_tests(run_search_parallel=run_search_parallel)
    results.to_csv("results-multithreading.csv", index=False)
    print(
        f"Test finalizado. Resultados guardados en {b.UNDERLINE}'results-multithreading.csv'{b.ENDC}"
    )


if __name__ == "__main__":
    main()
