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
        self.experiment_timings = list()
        self.total_timing = -1
        self.mean_timing = -1
        self.median_timing = -1
        self.is_index_type = is_index_type
        self.is_query_type = is_query_type

    def add_time(self, experiment_time: float):
        self.experiment_timings.append(experiment_time)

    def get_num_of_transactions(self) -> int:
        return len(self.experiment_timings)
    
    def compute_statistics(self):
        self.__sort_experiment_timings()
        self.__compute_total_timing()
        self.__compute_median_timing()
        self.__compute_mean_timing()
        
    def __sort_experiment_timings(self):
        self.experiment_timings.sort()
        
    def __compute_total_timing(self):
        length = self.get_num_of_transactions()
        sum = 0
        for i in range(length):
            sum += self.experiment_timings[i]
        self.total_timing = sum
        
    
    def __compute_median_timing(self):
        length = self.get_num_of_transactions() 
        if length == 0:
            self.median_timing = 0
        if length % 2 == 0:
            second_num = self.experiment_timings[length // 2]
            first_num = self.experiment_timings[length // 2 - 1]
            self.median_timing = (second_num + first_num) / 2
        else:
            self.median_timing = self.experiment_timings[(length - 1) // 2]
            
    def __compute_mean_timing(self):
        self.mean_timing = self.total_timing / self.get_num_of_transactions()

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
        print(f"Total time taken: ", self.total_timing)
        print(f"Mean: ", self.mean_timing)
        print(f"Median: ", self.median_timing)
        all_timing_str = "All timings: "
        for i in range(self.get_num_of_transactions()):
            all_timing_str += str(self.experiment_timings[i]) + ", "
        all_timing_str = all_timing_str[0: len(all_timing_str) - 2]
        print(all_timing_str)
        print("---------------------------------------------")
