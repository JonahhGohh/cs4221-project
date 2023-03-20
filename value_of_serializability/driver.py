from client import run_experiment
from psycopg2 import extensions


LIBRARY_ISOLATION_LEVELS = [extensions.ISOLATION_LEVEL_SERIALIZABLE, extensions.ISOLATION_LEVEL_REPEATABLE_READ, extensions.ISOLATION_LEVEL_SERIALIZABLE]

NUM_OF_SWAP_TRANSACTIONS = 1000
NUM_OF_THREADS = 20

def main():
    for level_int in LIBRARY_ISOLATION_LEVELS:
        run_experiment(NUM_OF_SWAP_TRANSACTIONS, NUM_OF_THREADS, level_int)

if __name__ == "__main__":
    main()

