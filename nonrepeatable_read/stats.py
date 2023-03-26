import time

class Statistics:
    
    def __init__(self):
        self.experiment_parameters = {}
        self.start_time = -1
        self.end_time = -1
        self.num_of_transactions = -1
        self.end_balance = -1

    def start_timer(self):
        self.start_time = time.perf_counter()

    def end_timer(self):
        self.end_time = time.perf_counter()

    def get_response_time(self):
        if self.start_time == -1 or self.end_time == -1:
            return -1
        else:
            return self.end_time - self.start_time

    def set_num_of_transactions(self, num_of_transactions):
        self.num_of_transactions = num_of_transactions

    def set_end_balance(self, end_balance):
        self.end_balance = end_balance
    
    def get_end_balance(self, end_balance):
        return self.end_balance

    def get_throughput(self):
        if self.num_of_transactions == -1:
            return -1
        else:
            return self.num_of_transactions/self.get_response_time()
    
    def set_experiment_parameters(self, experiment_parameters):
        self.experiment_parameters = experiment_parameters

    def print_experiment_setup(self):
        print("------------ EXPERIMENT SETTINGS ------------")
        print("Isolation Level: ", self.experiment_parameters["ISOLATION_LEVEL"])
        print("Select Query Type: ", self.experiment_parameters["SELECT_QUERY_TYPE"])
        print("Number of Threads: ", self.experiment_parameters["NUM_THREADS"])
        print("Start Balance: ", self.experiment_parameters["START_BALANCE"])
        print("Withdrawal Amount: ", self.experiment_parameters["WITHDRAWAL_AMOUNT"])
        print("---------------------------------------------")

    def print_results(self):
        print("---------------- STATISTICS -----------------")

        end_balance = self.get_end_balance()
        print('End Balance: ', end_balance if end_balance != -1 else "INVALID")
        print('\nBased on application level check implemented during withdrawal transaction,\nwithdrawal (UPDATE statement) should only be executed if there is sufficient prior balance (from SELECT statement).\nHence, end balance ideally should be >= 0, not negative, if no concurrency anomaly is encountered.')
        print('Is end balance valid? ', end_balance >= 0)


        response_time = self.get_response_time()
        throughput = self.get_throughput()
        print("Response Time: ", response_time if response_time != -1 else "INVALID")
        print("Throughput: ", throughput if throughput != -1 else "INVALID")
        
        print("---------------------------------------------")

