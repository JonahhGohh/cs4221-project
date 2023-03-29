from db import *

def run_experiment():
    setup_db()
    print("----------Experiment On Composite Indexes----------")

    start_time = time.perf_counter()
    get_first_name()
    end_time = time.perf_counter()
    print("Using first attribute of composite index \nTime Taken: ", end_time - start_time )
    get_first_name_execution_plan() # Uses Index



    start_time = time.perf_counter()
    get_last_name()
    end_time = time.perf_counter()
    print("\nUsing second attribute of composite index \nTime Taken: ", end_time - start_time )
    get_last_name_execution_plan() # Does not use Index -> Execution time is much larger

    print("-----------------End of Experiment-----------------")



if __name__ == '__main__':
    run_experiment()

    