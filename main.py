import warnings

warnings.filterwarnings("ignore")

import test_multiprocessing
import test_multithreading

if __name__ == "__main__":
    test_multithreading.main()
    test_multiprocessing.main()
