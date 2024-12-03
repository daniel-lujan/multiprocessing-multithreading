from multiprocessing import Pool
from time import perf_counter

from bcolors import bcolors as b
from common import run_tests


def search_for_value(args):
    file_name, value = args
    start = perf_counter()
    with open(file_name, "r") as file:
        for i, line in enumerate(file):
            if line.strip() == value:
                return perf_counter() - start, True
    return perf_counter() - start, False


def run_search_parallel(value: str, files: list[str]):
    with Pool(processes=len(files)) as pool:
        results = pool.map(search_for_value, [(file, value) for file in files])

    return results


def main():
    print(f"{b.HEADER}Ejecutando test con Multiprocessing...{b.ENDC}")
    results = run_tests(run_search_parallel=run_search_parallel)
    results.to_csv("results-multiprocessing.csv", index=False)
    print(
        f"Test finalizado. Resultados guardados en {b.UNDERLINE}'results-multiprocessing.csv'{b.ENDC}"
    )


if __name__ == "__main__":
    main()
