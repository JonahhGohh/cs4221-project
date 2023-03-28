from db import *

def run_experiment():
    setup_db()

    start_time = time.perf_counter()
    get_first_name()
    end_time = time.perf_counter()
    print("FIRST Result: ", end_time - start_time )


    start_time = time.perf_counter()
    get_last_name()
    end_time = time.perf_counter()
    print("SECOND Result: ", end_time - start_time )

    get_first_name_execution_plan()
    get_last_name_execution_plan()

    print("DONE")

if __name__ == '__main__':
    run_experiment()

    