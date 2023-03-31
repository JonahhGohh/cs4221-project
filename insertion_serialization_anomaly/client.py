from stats import Statistics
from db import *
from threading import Thread, Lock
from psycopg2 import extensions

NUM_THREADS = 20

# Number of transactions cannot be too large to prevent integer out of range error
NUM_OF_SUM_INSERT_TRANSACTION = 50
CONSTANT_SUM_INSERT_SUM = 6
NUM_SUM_INSERT_ROWS = 3
sum_insert_counter = 1
sum_insert_counter_lock = Lock()

# Must be a key from LIBRARY_ISOLATION_LEVELS
ISOLATION_LEVEL_STRING = "SERIALIZABLE"

LIBRARY_ISOLATION_LEVELS = {
  "READ_COMMITTED": extensions.ISOLATION_LEVEL_READ_COMMITTED,
  "REPEATABLE_READ": extensions.ISOLATION_LEVEL_REPEATABLE_READ,
  "SERIALIZABLE": extensions.ISOLATION_LEVEL_SERIALIZABLE,
}
ISOLATION_LEVEL = LIBRARY_ISOLATION_LEVELS[ISOLATION_LEVEL_STRING]

def print_experiment_settings():
  print("------------ EXPERIMENT SETTINGS ------------")
  print("Number of insert Transactions: ", NUM_OF_SUM_INSERT_TRANSACTION)
  print("Number of Threads: ", NUM_THREADS)
  print("Isolation Level: ", ISOLATION_LEVEL_STRING)
  print("---------------------------------------------")

def execute_sum_insert_client(results):
    sum_count = 0
    sum_correct_count = 0
    b_sum = sum_b(ISOLATION_LEVEL)
    # sum_insert_counter cannot be used to retrieve num of rows due to potential rollbacks
    b_rows = count_b(ISOLATION_LEVEL)
    sum_count += 1
    if b_sum == (CONSTANT_SUM_INSERT_SUM * (2 ** (b_rows - NUM_SUM_INSERT_ROWS))):
        sum_correct_count += 1
    results.append([sum_count, sum_correct_count])

def execute_sum_insert():
    global sum_insert_counter

    while True:
        sum_insert_counter_lock.acquire()
        if sum_insert_counter > NUM_OF_SUM_INSERT_TRANSACTION:
            sum_insert_counter_lock.release()
            break
        curr_counter = sum_insert_counter + NUM_SUM_INSERT_ROWS
        sum_insert_counter += 1
        sum_insert_counter_lock.release()
        sum_insert(ISOLATION_LEVEL, curr_counter)

def main():
  # setup db
  setup_db()

  # print the experiment settings
  print_experiment_settings()
  
  # create statistics object
  stats = Statistics()

  # execute insert threads
  results = []
  sum_insert_client_thread = Thread(target=execute_sum_insert_client, args=(results,))
  sum_insert_threads = list()

  for i in range(NUM_THREADS):
    sum_insert_thread = Thread(target=execute_sum_insert)
    sum_insert_threads.append(sum_insert_thread)
  
  stats.start_timer()
  for i in range(NUM_THREADS):
    sum_insert_threads[i].start()
  for i in range(NUM_THREADS):
    sum_insert_threads[i].join()
  sum_insert_client_thread.start()
  sum_insert_client_thread.join()
  stats.end_timer()
  (sum_count, sum_correct_count) = results[0]
  
  stats.set_sum_count(sum_count)
  stats.set_sum_correct_count(sum_correct_count)
  print('RESPONSE TIME:', stats.get_response_time())
  stats.print_stats()
if __name__ == '__main__':
    main()
    
