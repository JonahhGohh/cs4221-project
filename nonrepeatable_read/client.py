from threading import Thread, Lock
from psycopg2 import extensions
import time
from db import *


# Must be a key from LIBRARY_ISOLATION_LEVELS
ISOLATION_LEVEL_STRING = "READ_COMMITTED"
# Must be a key from SELECT_QUERY_TYPES
SELECT_TYPE_STRING = "NORMAL_SELECT"

SELECT_QUERY_TYPES = {
    "NORMAL_SELECT": "",
    "SELECT_FOR_UPDATE": " FOR UPDATE"
}
SELECT_QUERY_TYPE = SELECT_QUERY_TYPES[SELECT_TYPE_STRING]

LIBRARY_ISOLATION_LEVELS = {
    "READ_COMMITTED": extensions.ISOLATION_LEVEL_READ_COMMITTED,
    "REPEATABLE_READ": extensions.ISOLATION_LEVEL_REPEATABLE_READ,
    "SERIALIZABLE": extensions.ISOLATION_LEVEL_SERIALIZABLE,
}
ISOLATION_LEVEL = LIBRARY_ISOLATION_LEVELS[ISOLATION_LEVEL_STRING]

NUM_THREADS = 10
START_BALANCE = 200000
WITHDRAWAL_AMOUNT = 20
NUM_OF_TRANSACTION = START_BALANCE / WITHDRAWAL_AMOUNT * 1.1
transaction_counter = 0
transaction_counter_lock = Lock()


def print_experiment_settings():
    print("------------ EXPERIMENT SETTINGS ------------")
    print("Isolation Level: ", ISOLATION_LEVEL_STRING)
    print("Number of Threads: ", NUM_THREADS)
    print("Start Balance: ", START_BALANCE)
    print("Withdrawal Amount: ", WITHDRAWAL_AMOUNT)
    print("---------------------------------------------")


def execute_withdrawal_client():
    global transaction_counter

    while True:
        transaction_counter_lock.acquire()
        if transaction_counter >= NUM_OF_TRANSACTION:
            transaction_counter_lock.release()
            break

        transaction_counter += 1
        transaction_counter_lock.release()
        withdrawal_transaction(
            ISOLATION_LEVEL, SELECT_QUERY_TYPE, WITHDRAWAL_AMOUNT)


def main():
    setup_db()
    print_experiment_settings()

    reset_balance(START_BALANCE)

    withdrawal_threads = list()

    for i in range(NUM_THREADS):
        withdrawal_thread = Thread(target=execute_withdrawal_client)
        withdrawal_threads.append(withdrawal_thread)

    # start timer
    start_time = time.perf_counter()
    for i in range(NUM_THREADS):
        withdrawal_threads[i].start()

    for i in range(NUM_THREADS):
        withdrawal_threads[i].join()

    # check correctness
    end_balance = find_end_balance()
    print('End balance: ', end_balance)
    print('\nBased on application level check implemented during withdrawal transaction,\nwithdrawal (UPDATE statement) should only be executed if there is sufficient prior balance (from SELECT statement).\nHence, end balance ideally should be >= 0 (not negative) if no concurrency anomaly is encountered.')
    print('Is end balance valid? ', end_balance >= 0)

    # end timer
    end_time = time.perf_counter()

    # calculate throughput
    response_time = end_time - start_time
    print('response time in seconds', response_time)


if __name__ == '__main__':
    main()
