# Multiprocessing vs Multithreading

> [!IMPORTANT]
> This an academic repository, developed for Operative Systems course from Universidad de Antioquia.

This repository implements tests to compare multiprocessing against multithreading for parallel file search.

## Files description

### Notebooks

- **dataset-generation.ipynb**: Generation of random test data. Generate several CSV files with random UUIDs (version 4), and save them into the `/data` directory.
- **analysis.ipynb**: Reads the test results to plot relevant charts, and generating ANOVA table.

### Scripts

- **main.py**: Tests entrypoint. This generates the target values to search for and performs parallel tests using both multiprocessing and multithreading.
- **common.py**: Shared functions (like file search, test logic, etc) between multiprocessing and multithreading.
- **test_multiprocessing.py**: Custom implementation of parallel search using multiprocessing Pool.
- **test_multithreading.py**: Custom implementation of parallel search using Threads.
- **bcolors.py**: Utility for printing colors to the stdout.
- **requirements.txt**: Python dependencies.

## Steps to reproduce

> [!NOTE] Prerrequisites: You will need Python (version 3.11 or later recommended) and pip installed.

### 1. Create virtual environment

> [!TIP] This is not mandatory, but recommended.

```bash
python -m pip install --user virtualenv
python -m venv venv

source /venv/bin/activate # For Linux
./venv/Scripts/activate # For Windows
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Generate test dataset

Read `notebooks/dataset-generation.ipynb` for instructions.

### 4. Configure tests (optional)

In `common.py` you can configure directory variables, and the random tests that the script will execute.

```python
data_directory = "./data/"
 target_values_dir = "./temp/target_values/"
RANDOM_TESTS = 40
```

### 5. Run tests

```bash
python main.py
```

### 6. See results

Read and execute `notebooks/dataset-generation.ipynb`.
