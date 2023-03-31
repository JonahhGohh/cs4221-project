from client import run_experiment
from psycopg2 import extensions


EXPERIMENT_SETUP = [
    {
        "ISOLATION_LEVEL": "READ_COMMITTED",
        "NUM_THREADS": 20
    },
    {
        "ISOLATION_LEVEL": "REPEATABLE_READ",
        "NUM_THREADS": 20
    },
    {
        "ISOLATION_LEVEL": "SERIALIZABLE",
        "NUM_THREADS": 20
    },
    {
        "ISOLATION_LEVEL": "READ_COMMITTED",
        "NUM_THREADS": 50
    },
    {
        "ISOLATION_LEVEL": "REPEATABLE_READ",
        "NUM_THREADS": 50
    },
    {
        "ISOLATION_LEVEL": "SERIALIZABLE",
        "NUM_THREADS": 50
    },
]

NUM_OF_SWAP_TRANSACTIONS = 10000

def main():
    stats_list = []
    for experiment_parameters in EXPERIMENT_SETUP:
        stats = run_experiment(**experiment_parameters, NUM_OF_SWAP_TRANSACTIONS = NUM_OF_SWAP_TRANSACTIONS)
        stats_list.append(stats)

    # Print stats only when all experiments have finished running    
    for stats in stats_list:
        stats.print_experiment_setup()
        stats.print_results()


if __name__ == "__main__":
    main()

