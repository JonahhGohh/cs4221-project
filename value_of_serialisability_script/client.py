from serialisability_1 import sum_b
from serialisability_1 import transfer_b
from threading import Thread
from random import randint

NUM_THREADS = 20
LENGTH = 1000

def spawn_sum_clients():
  threads = list()
  for i in range(NUM_THREADS):
    thread = Thread(target = sum_b)
    threads.append(thread)
  for i in range(NUM_THREADS):
    threads[i].join()

def spawn_transfer_clients():
  threads = list()
  for i in range(NUM_THREADS):
    rand_id = randint(1, LENGTH)
    thread = Thread(target = transfer_b, args = (rand_id, LENGTH))
    threads.append(thread)
  for i in range(NUM_THREADS):
    threads[i].join()
    