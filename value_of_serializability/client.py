from db import *
from threading import Thread, Lock
from random import randint
from psycopg2 import extensions

NUM_THREADS = 20
NUM_OF_ROWS_IN_DATA = 100000
id_counter = 1
END_FLAG = False
CONSTANT_SUM = 500000500000
NUM_OF_SWAP_TRANSACTION = 1000
id_counter_lock = Lock()
end_flag_lock = Lock()

LIBRARY_ISOLATION_LEVELS = {
  "READ_COMMITTED": extensions.ISOLATION_LEVEL_READ_COMMITTED,
  "REPEATABLE_READ": extensions.ISOLATION_LEVEL_REPEATABLE_READ,
  "SERIALIZABLE": extensions.ISOLATION_LEVEL_SERIALIZABLE,
}
ISOLATION_LEVEL = LIBRARY_ISOLATION_LEVELS["SERIALIZABLE"]

def execute_sum_client(results):
  global END_FLAG
  global ISOLATION_LEVEL
  global CONSTANT_SUM
  global end_flag_lock

  sum_count = 0
  sum_correct_count = 0
  while True:
    end_flag_lock.acquire()
    if END_FLAG:
      end_flag_lock.release()
      break
    end_flag_lock.release()
    sum_count += 1
    result = sum_b(ISOLATION_LEVEL)
    if result == CONSTANT_SUM:
      sum_correct_count += 1

  results.append([sum_count, sum_correct_count])
    
def execute_swap_client():
  global NUM_OF_SWAP_TRANSACTION
  global ISOLATION_LEVEL
  global END_FLAG
  global id_counter
  global id_counter_lock
  global end_flag_lock

  while True:
    id_counter_lock.acquire()
    if id_counter >= NUM_OF_SWAP_TRANSACTION * 2:
      id_counter_lock.release()
      break
    id_counter += 2
    id_counter_lock.release()
    swap_b(ISOLATION_LEVEL, id_counter)
  end_flag_lock.acquire()
  if END_FLAG == False:
    END_FLAG = True
  end_flag_lock.release()
    
def main():
  global NUM_THREADS
  
  # setup db
  setup_db()

  # execute the sum_thread
  results = []
  sum_thread = Thread(target=execute_sum_client, args=(results,))

  swap_threads = list()

  # execute all the swap threads
  for i in range(NUM_THREADS):
    swap_thread = Thread(target=execute_swap_client)
    swap_threads.append(swap_thread)

  # start timer
  sum_thread.start()
  for i in range(NUM_THREADS):
    swap_threads[i].start()
  for i in range(NUM_THREADS):
    swap_threads[i].join()
  # end timer
  sum_thread.join()
  (sum_count, sum_correct_count) = results[0]
  print('sum_count', sum_count)
  print('sum_correct_count', sum_correct_count)
  
if __name__ == '__main__':
    main()
    
