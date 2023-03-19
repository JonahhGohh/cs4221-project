import time

class Statistics:
    
    def __init__(self):
        self.start_time = -1
        self.end_time = -1
        self.sum_count = -1
        self.sum_correct_count = -1

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

    def get_ratio_correct_count(self):
        if self.sum_correct_count == -1 or self.sum_count == -1:
            return -1
        else:
            return self.sum_correct_count/self.sum_count

    def get_throughput(self):
        if self.start_time == -1 or self.end_time == -1 or self.sum_count == -1:
            return -1
        else:
            return self.sum_count/self.get_response_time()
    
    def print_stats(self):
        ratio_correct_sum = self.get_ratio_correct_count()
        print("Ratio of Correct Sum Count: ", ratio_correct_sum if ratio_correct_sum != -1 else "INVALID")

        throughput = self.get_throughput()
        print("Throughput: ", throughput if throughput != -1 else "INVALID")
