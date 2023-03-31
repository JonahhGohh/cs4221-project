from threading import Thread, Lock
from psycopg2 import extensions
from stats import Statistics
from db import *

CONSTANT_SUM = 500000500000
id_counter = 1
end_flag = False
id_counter_lock = Lock()

LIBRARY_ISOLATION_LEVELS = {
    "READ_COMMITTED": extensions.ISOLATION_LEVEL_READ_COMMITTED,
    "REPEATABLE_READ": extensions.ISOLATION_LEVEL_REPEATABLE_READ,
    "SERIALIZABLE": extensions.ISOLATION_LEVEL_SERIALIZABLE,
}

def execute_sum_client(isolation_level, results):
  sum_count = 0
  sum_correct_count = 0
  while not end_flag:
    result = sum_b(LIBRARY_ISOLATION_LEVELS[isolation_level])
    # if value is not read from the db, continue and try again
    if result == 0:
      continue
    sum_count += 1
    if result == CONSTANT_SUM:
      sum_correct_count += 1

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
    swap_b(LIBRARY_ISOLATION_LEVELS[isolation_level], curr_id_counter)
    

def run_experiment(ISOLATION_LEVEL, NUM_THREADS, NUM_OF_SWAP_TRANSACTIONS):
  global end_flag
  
  # setup db
  setup_db()
  reset_global_parameters()

  # create statistics object
  stats = Statistics()
  stats.set_experiment_parameters({"ISOLATION_LEVEL": ISOLATION_LEVEL,
                                  "NUM_THREADS": NUM_THREADS, "NUM_OF_SWAP_TRANSACTIONS": NUM_OF_SWAP_TRANSACTIONS})

  # execute the sum_thread
  sum_results = []
  sum_thread = Thread(target=execute_sum_client, args=(ISOLATION_LEVEL, sum_results,))
  swap_threads = list()

  # execute all the swap threads
  for i in range(NUM_THREADS):
    swap_thread = Thread(target=execute_swap_client, args=(ISOLATION_LEVEL, NUM_OF_SWAP_TRANSACTIONS))
    swap_threads.append(swap_thread)

  sum_thread.start()
  # start timer
  stats.start_timer()
  for i in range(NUM_THREADS):
    swap_threads[i].start()

  for i in range(NUM_THREADS):
    swap_threads[i].join()
  end_flag = True

  # end timer
  stats.end_timer()
  sum_thread.join()
  (sum_count, sum_correct_count) = sum_results[0]
  stats.set_sum_count(sum_count)
  stats.set_sum_correct_count(sum_correct_count)

  return stats

def reset_global_parameters():
  global id_counter, end_flag
  id_counter = 1
  end_flag = False

