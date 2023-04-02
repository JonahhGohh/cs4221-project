import time

class Statistics:
    
    def __init__(self):
        self.experiment_parameters = {}
        self.start_time = -1
        self.end_time = -1
        self.sum_count = -1
        self.sum_correct_count = -1
        self.num_of_swap_transactions = -1

    def start_timer(self):
        self.start_time = time.perf_counter()

    def end_timer(self):
        self.end_time = time.perf_counter()

    def get_response_time(self):
        if self.start_time == -1 or self.end_time == -1:
            return -1
        else:
            return self.end_time - self.start_time

    def set_sum_count(self, sum_count):
        self.sum_count = sum_count

    def set_sum_correct_count(self, sum_correct_count):
        self.sum_correct_count = sum_correct_count

    def set_experiment_parameters(self, experiment_parameters):
        self.experiment_parameters = experiment_parameters
        self.num_of_swap_transactions = experiment_parameters["NUM_OF_SWAP_TRANSACTIONS"]

    def get_ratio_correct_count(self):
        if self.sum_correct_count == -1 or self.sum_count == -1:
            return -1
        else:
            return self.sum_correct_count/self.sum_count
    
    def get_sum_count(self):
        if self.sum_count == -1:
            return -1
        else:
            return self.sum_count

    def get_throughput(self):
        if self.num_of_swap_transactions == -1:
            return -1
        else:
            return self.num_of_swap_transactions/self.get_response_time()
    
    def print_experiment_setup(self):
        print("------------ EXPERIMENT SETTINGS ------------")
        print("Number of Swap Transactions: ", self.num_of_swap_transactions)
        print("Isolation Level: ", self.experiment_parameters["ISOLATION_LEVEL"])
        print("Number of Threads: ", self.experiment_parameters["NUM_THREADS"])
        print("---------------------------------------------")

    def print_results(self):
        print("---------------- STATISTICS -----------------")

        ratio_correct_sum = self.get_ratio_correct_count()
        print("Ratio of Correct Sum Count: ", round(ratio_correct_sum, 4) if ratio_correct_sum != -1 else "INVALID")

        throughput = self.get_throughput()
        print("Swap Throughput: ", f"{round(throughput, 2)} transaction/second" if throughput != -1 else "INVALID")
        sum_count = self.get_sum_count()
        print("Number of Sum Transactions: ", sum_count if sum_count != -1 else "INVALID")
        
        print("---------------------------------------------")

