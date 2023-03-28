from db import *
from stats import *

NUM_OF_QUERIES = 100
NUM_OF_INSERT = 100
NUM_OF_EXPERIMENTS = 10
LAST_ID_OF_SEED = 1000000

def run_experiments():
    experiment_stats = []

    print('Running all experiments! Please Wait!')
    # Test non index query
    non_index_query_stats = Statistics(False, True)
    for i in range(NUM_OF_EXPERIMENTS):
        setup_db()
        non_index_query_timer = Timer()
        non_index_query_timer.start_timer()
        query_age_range(NUM_OF_QUERIES)
        non_index_query_timer.end_timer()
        non_index_query_stats.add_time(non_index_query_timer.get_response_time())
    experiment_stats.append(non_index_query_stats)
    print("non index query experiment completed!")
    # Test non index insert
    non_index_insert_stats = Statistics(False, False)
    for i in range(NUM_OF_EXPERIMENTS):
        setup_db()
        non_index_insert_timer = Timer()
        non_index_insert_timer.start_timer()
        insert_new_rows(NUM_OF_INSERT, LAST_ID_OF_SEED)
        non_index_insert_timer.end_timer()
        non_index_insert_stats.add_time(non_index_insert_timer.get_response_time())
    experiment_stats.append(non_index_insert_stats)
    print("non index insert experiment completed!")
    
    # Test index query
    index_query_stats = Statistics(True, True)
    for i in range(NUM_OF_EXPERIMENTS):
        setup_db(True)
        index_query_timer = Timer()
        index_query_timer.start_timer()
        query_age_range(NUM_OF_QUERIES)
        index_query_timer.end_timer()
        index_query_stats.add_time(index_query_timer.get_response_time())
    experiment_stats.append(index_query_stats)
    print("index query experiment completed!")
    
    # Test index insert
    index_insert_stats = Statistics(True, False)
    for i in range(NUM_OF_EXPERIMENTS):
        setup_db(True)
        index_insert_timer = Timer()
        index_insert_timer.start_timer()
        insert_new_rows(NUM_OF_INSERT, LAST_ID_OF_SEED)
        index_insert_timer.end_timer()
        index_insert_stats.add_time(index_insert_timer.get_response_time())
    experiment_stats.append(index_insert_stats)
    print("index insert experiment completed!")
    
    print('Finished experiments...')

    return experiment_stats


def print_experiment_stats(experiment_stats):
    for stats in experiment_stats:
        stats.print_experiment_setup()
        stats.compute_statistics()
        stats.print_results()


def main():
    experiment_stats = run_experiments()
    print_experiment_stats(experiment_stats)


if __name__ == '__main__':
    main()
