from serialisability_1 import *
from threading import Thread, Lock
from random import randint

NUM_THREADS = 20
NUM_OF_ROWS_IN_DATA = 1000
id_counter = 0
END_FLAG = False
CONSTANT_SUM = 1000000
NUM_OF_SWAP_TRANSACTION = 1000
id_counter_lock = Lock()
end_flag_lock = Lock()

def execute_sum_client():
  sum_count = 0
  sum_correct_count = 0
  # END_FLAG should not need to be wrapped a mutex because only 1 swap thread writes and 1 sum thread reads
  while True:
    end_flag_lock.acquire()
    if END_FLAG:
      end_flag_lock.release()
      break
    end_flag_lock.release()
    sum_count += 1
    result = sum_b()
    if result == CONSTANT_SUM:
      sum_correct_count += 1
  return (sum_count, sum_correct_count)
    
def execute_swap_client():
  # wrap COUNTER variable in mutex
  while True:
    id_counter_lock.acquire()
    if id_counter >= NUM_OF_SWAP_TRANSACTION * 2:
      id_counter_lock.release()
      break
    id_counter += 2
    id_counter_lock.release()
    swap_b(id_counter, NUM_OF_ROWS_IN_DATA)
  # wrap in mutex
  end_flag_lock.acquire()
  if END_FLAG == False:
    END_FLAG = True
  end_flag_lock.release()
    
def client(num_threads):
  # setup db
  setup_db()
  # execute the sum_thread
  sum_thread = Thread(target=execute_sum_client)
  # execute all the swap threads
  # start timer
  swap_threads = list()
  for i in range(NUM_THREADS):
    swap_thread = Thread(target=execute_swap_client)
    swap_threads.append(swap_thread)
  for i in range(NUM_THREADS):
    swap_threads[i].join()
  # end timer
  sum_thread.join()
  (sum_count, sum_correct_count) = sum_thread.result
  
  
    