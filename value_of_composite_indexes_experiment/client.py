from db import *

def run_experiment():
    setup_db()
    print("----------Experiment On Composite Indexes----------")

    print("Using first attribute of composite index")
    get_first_name_execution_plan() # Uses Index


    print("\nUsing last attribute of composite index")
    get_last_name_execution_plan() # Does not use Index


    print("\nUsing an equality check for one part of condition")
    get_first_name_last_name_using_index_execution_plan() # Use Index


    print("\nUsing both inequality checks for both parts of condition")
    get_first_name_last_name_not_using_index_execution_plan() # Does not use Index

    print("-----------------End of Experiment-----------------")



if __name__ == '__main__':
    run_experiment()

    