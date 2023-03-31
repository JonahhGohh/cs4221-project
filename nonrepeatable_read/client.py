from threading import Thread, Lock
from psycopg2 import extensions
from db import *
from stats import Statistics


EXPERIMENT_SETUP = [
    {
        # Must be a key from LIBRARY_ISOLATION_LEVELS
        "ISOLATION_LEVEL": "READ_COMMITTED",
        # Must be a key from SELECT_QUERY_TYPES
        "SELECT_QUERY_TYPE": "NORMAL_SELECT",
        # Must be >= 2 to simulate concurrent transactions
        "NUM_THREADS": 5
    },
    {
        "ISOLATION_LEVEL": "READ_COMMITTED",
        "SELECT_QUERY_TYPE": "SELECT_FOR_UPDATE",
        "NUM_THREADS": 5
    },
    {
        "ISOLATION_LEVEL": "REPEATABLE_READ",
        "SELECT_QUERY_TYPE": "NORMAL_SELECT",
        "NUM_THREADS": 5
    },
    {
        "ISOLATION_LEVEL": "SERIALIZABLE",
        "SELECT_QUERY_TYPE": "NORMAL_SELECT",
        "NUM_THREADS": 5
    },
    {
        "ISOLATION_LEVEL": "READ_COMMITTED",
        "SELECT_QUERY_TYPE": "NORMAL_SELECT",
        "NUM_THREADS": 30
    },
    {
        "ISOLATION_LEVEL": "READ_COMMITTED",
        "SELECT_QUERY_TYPE": "SELECT_FOR_UPDATE",
        "NUM_THREADS": 30
    },
    {
        "ISOLATION_LEVEL": "REPEATABLE_READ",
        "SELECT_QUERY_TYPE": "NORMAL_SELECT",
        "NUM_THREADS": 30
    },
    {
        "ISOLATION_LEVEL": "SERIALIZABLE",
        "SELECT_QUERY_TYPE": "NORMAL_SELECT",
        "NUM_THREADS": 30
    },
]

SELECT_QUERY_TYPES = {
    "NORMAL_SELECT": "",
    "SELECT_FOR_UPDATE": " FOR UPDATE"
}
select_query_type = None

LIBRARY_ISOLATION_LEVELS = {
    "READ_COMMITTED": extensions.ISOLATION_LEVEL_READ_COMMITTED,
    "REPEATABLE_READ": extensions.ISOLATION_LEVEL_REPEATABLE_READ,
    "SERIALIZABLE": extensions.ISOLATION_LEVEL_SERIALIZABLE,
}
isolation_level = None

num_threads = 0
START_BALANCE = 200000
WITHDRAWAL_AMOUNT = 20
# This should be kept constant across the different experiments
# and should cause transactions to continue executing even after balance reach 0
# to check if the experiment setup will result in overdeduction of balance due to concurrency issue
NUM_OF_TRANSACTION = START_BALANCE / WITHDRAWAL_AMOUNT * 1.1
transaction_counter = 0
transaction_counter_lock = Lock()
transaction_retries = 0
transaction_retries_lock = Lock()


def execute_withdrawal_client():
    global transaction_counter
    global transaction_retries

    while True:
        transaction_counter_lock.acquire()
        if transaction_counter >= NUM_OF_TRANSACTION:
            transaction_counter_lock.release()
            break

        transaction_counter += 1
        transaction_counter_lock.release()
        retries = withdrawal_transaction(
            isolation_level, select_query_type, WITHDRAWAL_AMOUNT)
        if retries > 0:
            transaction_retries_lock.acquire()
            transaction_retries += retries
            transaction_retries_lock.release()


def reset_global_parameters(experiment_parameters):
    global transaction_counter
    global transaction_retries
    global num_threads
    global isolation_level
    global select_query_type

    transaction_counter = 0
    transaction_retries = 0
    num_threads = experiment_parameters["NUM_THREADS"]
    isolation_level = LIBRARY_ISOLATION_LEVELS[experiment_parameters["ISOLATION_LEVEL"]]
    select_query_type = SELECT_QUERY_TYPES[experiment_parameters["SELECT_QUERY_TYPE"]]


def run_experiments():
    experiment_stats = []

    print('Running all experiments... Please Wait...')
    print('You may see many serialization errors from dbms if isolation level = REPEATABLE_READ or SERIALIZABLE, this is normal, do not be alarmed')
    for experiment_parameters in EXPERIMENT_SETUP:
        setup_db()
        stats = Statistics()
        stats.set_experiment_parameters({**experiment_parameters, "NUM_OF_TRANSACTION": NUM_OF_TRANSACTION,
                                        "START_BALANCE": START_BALANCE, "WITHDRAWAL_AMOUNT": WITHDRAWAL_AMOUNT})
        reset_global_parameters(experiment_parameters)
        reset_balance(START_BALANCE)

        withdrawal_threads = list()
        for i in range(num_threads):
            withdrawal_thread = Thread(target=execute_withdrawal_client)
            withdrawal_threads.append(withdrawal_thread)

        stats.start_timer()

        # Concurrently execute transactions
        for i in range(num_threads):
            withdrawal_threads[i].start()

        for i in range(num_threads):
            withdrawal_threads[i].join()

        stats.end_timer()

        # Get end balance to check correctness
        stats.set_end_balance(find_end_balance())
        stats.set_transaction_retries(transaction_retries)

        experiment_stats.append(stats)
        print('Finished an experiment...')

    return experiment_stats


def print_experiment_stats(experiment_stats):
    for stats in experiment_stats:
        stats.print_experiment_setup()
        stats.print_results()


def main():
    experiment_stats = run_experiments()
    print_experiment_stats(experiment_stats)


if __name__ == '__main__':
    main()
