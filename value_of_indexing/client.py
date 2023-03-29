from db import *
from stats import *

NUM_OF_QUERIES = 100
NUM_OF_INSERT = 100
LAST_ID_OF_SEED = 1000000

def run_experiments():
    experiment_stats = []

    print('Running all experiments! Please Wait!')
    # Test non index query
    experiment_stats.append(execute_non_index_query())
    print("non index query experiment completed!")

    # Test non index insert
    experiment_stats.append(execute_non_index_insert())
    print("non index insert experiment completed!")
    
    # Test index query
    experiment_stats.append(execute_index_query())
    print("index query experiment completed!")
    
    # Test index insert
    experiment_stats.append(execute_index_insert())
    print("index insert experiment completed!")
    
    print('Finished experiments...')

    return experiment_stats

def execute_non_index_query():
    non_index_query_stats = Statistics(False, True)
    setup_db()
    non_index_query_timer = Timer()
    non_index_query_timer.start_timer()
    query_age_range(NUM_OF_QUERIES)
    non_index_query_timer.end_timer()
    non_index_query_stats.add_time(non_index_query_timer.get_response_time(), NUM_OF_QUERIES)
    return non_index_query_stats

def execute_non_index_insert():
    non_index_insert_stats = Statistics(False, False)
    setup_db()
    non_index_insert_timer = Timer()
    non_index_insert_timer.start_timer()
    insert_new_rows(NUM_OF_INSERT, LAST_ID_OF_SEED)
    non_index_insert_timer.end_timer()
    non_index_insert_stats.add_time(non_index_insert_timer.get_response_time(), NUM_OF_INSERT)
    return non_index_insert_stats

def execute_index_query():
    index_query_stats = Statistics(True, True)
    setup_db(True)
    index_query_timer = Timer()
    index_query_timer.start_timer()
    query_age_range(NUM_OF_QUERIES)
    index_query_timer.end_timer()
    index_query_stats.add_time(index_query_timer.get_response_time(), NUM_OF_QUERIES)
    return index_query_stats

def execute_index_insert():
    index_insert_stats = Statistics(True, False)
    setup_db(True)
    index_insert_timer = Timer()
    index_insert_timer.start_timer()
    insert_new_rows(NUM_OF_INSERT, LAST_ID_OF_SEED)
    index_insert_timer.end_timer()
    index_insert_stats.add_time(index_insert_timer.get_response_time(), NUM_OF_INSERT)
    return index_insert_stats


def print_experiment_stats(experiment_stats):
    for stats in experiment_stats:
        stats.print_experiment_setup()
        stats.print_results()


def main():
    experiment_stats = run_experiments()
    print_experiment_stats(experiment_stats)


if __name__ == '__main__':
    main()
