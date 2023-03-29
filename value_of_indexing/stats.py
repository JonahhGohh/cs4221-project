import time

class Timer:
    def __init__(self):
        self.start_time = -1
        self.end_time = -1
    
    def start_timer(self):
        self.start_time = time.perf_counter()
    
    def end_timer(self):
        self.end_time = time.perf_counter()
        
    def get_response_time(self) -> float:
        return self.end_time - self.start_time

class Statistics:
    def __init__(self, is_index_type: bool, is_query_type: bool):
        self.experiment_timing = -1
        self.num_of_queries = -1
        self.mean_time_for_one_query = -1
        self.is_index_type = is_index_type
        self.is_query_type = is_query_type

    def add_time(self, experiment_time: float, num_of_queries: int):
        self.experiment_timing = experiment_time
        self.num_of_queries = num_of_queries
        self.mean_time_for_one_query = experiment_time / num_of_queries

    def print_experiment_setup(self):
        print("------------ EXPERIMENT SETTINGS ------------")
        print("Value of Indexing experiment")
        if self.is_index_type:
            print("Index[X]")
        else:
            print("Index[]")
        if self.is_query_type:
            print("Query[X], Insert[]")
        else:
            print("Query[], Insert[X]")
        print("---------------------------------------------")

    def print_results(self):
        print("---------------- STATISTICS -----------------")
        experiment_type = "query" if self.is_query_type else "insert"
        print(f"Total number of {experiment_type} transactions: ", self.get_num_of_transactions())
        print(f"Total time taken: ", self.experiment_timing)
        print(f"Mean time for 1 query: ", self.mean_time_for_one_query)
        print("---------------------------------------------")
