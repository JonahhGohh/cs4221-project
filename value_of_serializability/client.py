from stats import Statistics
from db import *
from threading import Thread, Lock
from psycopg2 import extensions

CONSTANT_SUM = 500000500000
id_counter = 1
end_flag = False
id_counter_lock = Lock()

def print_experiment_settings(num_of_swap_transactions, num_threads, isolation_level):
  print("------------ EXPERIMENT SETTINGS ------------")
  print("Number of Swap Transactions: ", num_of_swap_transactions)
  print("Number of Threads: ", num_threads)
  print("Isolation Level: ", isolation_level)
  print("---------------------------------------------")

# Must be a key from LIBRARY_ISOLATION_LEVELS
ISOLATION_LEVEL_STRING = "READ_COMMITTED"

LIBRARY_ISOLATION_LEVELS = {
  "READ_COMMITTED": extensions.ISOLATION_LEVEL_READ_COMMITTED,
  "REPEATABLE_READ": extensions.ISOLATION_LEVEL_REPEATABLE_READ,
  "SERIALIZABLE": extensions.ISOLATION_LEVEL_SERIALIZABLE,
}
ISOLATION_LEVEL = LIBRARY_ISOLATION_LEVELS[ISOLATION_LEVEL_STRING]


def execute_sum_client(isolation_level, results):
  sum_count = 0
  sum_correct_count = 0
  while not end_flag:
    result = sum_b(isolation_level)
    # if value is not read from the db, continue and try again
    if result == 0:
      continue
    sum_count += 1
    if result == CONSTANT_SUM:
      sum_correct_count += 1

  print(sum_correct_count)
  print(sum_count)
  results.append([sum_count, sum_correct_count])
    
def execute_swap_client(isolation_level, num_of_swap_transactions):
  global id_counter

  while True:
    id_counter_lock.acquire()
    if id_counter >= num_of_swap_transactions * 2:
      id_counter_lock.release()
      break
    curr_id_counter = id_counter
    id_counter += 2
    id_counter_lock.release()
    swap_b(isolation_level, curr_id_counter)
    

def run_experiment(num_of_swap_transactions, num_threads, isolation_level):
  global end_flag
  
  # setup db
  setup_db()

  # print the experiment settings
  print_experiment_settings(num_of_swap_transactions, num_threads, isolation_level)
  
  # execute the sum_thread
  results = []
  sum_thread = Thread(target=execute_sum_client, args=(isolation_level, results,))
  swap_threads = list()

  # create statistics object
  stats = Statistics()

  # execute all the swap threads
  for i in range(num_threads):
    swap_thread = Thread(target=execute_swap_client, args=(isolation_level, num_of_swap_transactions))
    swap_threads.append(swap_thread)

  sum_thread.start()
  # start timer
  stats.start_timer()
  for i in range(num_threads):
    swap_threads[i].start()
  for i in range(num_threads):
    swap_threads[i].join()
  end_flag = True
  # end timer
  stats.end_timer()
  sum_thread.join()
  (sum_count, sum_correct_count) = results[0]

  stats.set_num_of_swap_transactions(num_of_swap_transactions)
  stats.set_sum_count(sum_count)
  stats.set_sum_correct_count(sum_correct_count)
  stats.print_stats()  


# if __name__ == '__main__':
#     run_experiment(1000, 20, extensions.ISOLATION_LEVEL_SERIALIZABLE)
    
