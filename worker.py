# worker.py

from yadtq import YADTQWorker
import time

def add(a, b):
    time.sleep(10)  # Simulate a long-running task
    return a + b

def sub(a, b):
    time.sleep(10)  # Simulate a long-running task
    return a - b

def multiply(a, b):
    time.sleep(10)  # Simulate a long-running task
    return a * b

def main():
    worker = YADTQWorker(group_id='worker-group1')
    task_functions = {
        'add': add,
        'sub': sub,
        'multiply': multiply
    }
    print(f"Worker {worker.worker_id} started")
    worker.consume_tasks(task_functions)

if __name__ == '__main__':  
    main()
