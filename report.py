import redis
from kafka import KafkaConsumer
from rich.console import Console
from rich.table import Table
import time
console = Console()

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

def display_tasks():
    table = Table(title="Task Monitoring")

    table.add_column("Task ID", justify="right", style="cyan")
    table.add_column("Status", justify="center", style="magenta")
    table.add_column("Worker ID", justify="center", style="green")

    # Get queued tasks
    for key in redis_client.scan_iter():
        task_info = redis_client.hgetall(key)
        table.add_row(key, task_info.get('status', 'unknown'), task_info.get('worker', 'N/A'))

    console.print(table)

def display_workers():
    table = Table(title="Worker Monitoring")

    table.add_column("Worker ID", justify="right", style="cyan")
    table.add_column("Tasks Completed", justify="center", style="magenta")
    table.add_column("Last Heartbeat", justify="center", style="green")

    # Simulate worker stats (from Redis)
    worker_stats = redis_client.hgetall("worker_stats")
    for worker, task_count in worker_stats.items():
        last_heartbeat = redis_client.get(f"worker:{worker}:heartbeat")
        table.add_row(worker, task_count, last_heartbeat)

    console.print(table)

if __name__ == '__main__':
    while True:
        console.clear()
        display_tasks()
        display_workers()
        time.sleep(5)  # Refresh every 5 seconds
