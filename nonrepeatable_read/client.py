from threading import Thread, Lock
from psycopg2 import extensions


# Must be a key from LIBRARY_ISOLATION_LEVELS
ISOLATION_LEVEL_STRING = "READ_COMMITTED"
# Must be a key from SELECT_QUERY_TYPES
SELECT_QUERY_TYPE = "NORMAL_SELECT"

SELECT_QUERY_TYPES = {
   "NORMAL_SELECT": "",
   "SELECT_FOR_UPDATE": " FOR UPDATE"
}

LIBRARY_ISOLATION_LEVELS = {
  "READ_COMMITTED": extensions.ISOLATION_LEVEL_READ_COMMITTED,
  "REPEATABLE_READ": extensions.ISOLATION_LEVEL_REPEATABLE_READ,
  "SERIALIZABLE": extensions.ISOLATION_LEVEL_SERIALIZABLE,
}
ISOLATION_LEVEL = LIBRARY_ISOLATION_LEVELS[ISOLATION_LEVEL_STRING]

NUM_THREADS = 10
START_BALANCE = 10000
WITHDRAWAL_AMOUNT = 10
NUM_OF_TRANSACTION = START_BALANCE / WITHDRAWAL_AMOUNT * 1.1

def print_experiment_settings():
  print("------------ EXPERIMENT SETTINGS ------------")
  print("Isolation Level: ", ISOLATION_LEVEL_STRING)
  print("Number of Threads: ", NUM_THREADS)
  print("Start Balance: ", START_BALANCE)
  print("Withdrawal Amount: ", WITHDRAWAL_AMOUNT)
  print("---------------------------------------------")



    
    

def main():
   return

if __name__ == '__main__':
    main()
    
