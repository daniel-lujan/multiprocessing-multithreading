import warnings

warnings.filterwarnings("ignore")

from os import remove

import test_multiprocessing
import test_multithreading
from bcolors import bcolors as b
from common import RANDOM_TESTS, generate_target_values, target_values_dir

if __name__ == "__main__":
    for i in range(RANDOM_TESTS):
        values = generate_target_values()
        values.to_csv(target_values_dir + f"target_values_{i}.csv", index=False)

    print(
        f"{b.OKGREEN}Target values values generated and saved in {target_values_dir}{b.ENDC}"
    )
    test_multithreading.main()
    test_multiprocessing.main()

    # Remove target values
    for i in range(RANDOM_TESTS):
        remove(target_values_dir + f"target_values_{i}.csv")
