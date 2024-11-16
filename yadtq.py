# yadtq.py
from kafka.admin import KafkaAdminClient, NewTopic
import uuid
import json
import threading
import time
import hashlib
from kafka import KafkaProducer, KafkaConsumer
import redis
admin_client=KafkaAdminClient(bootstrap_servers='localhost:9092')
def create_topic(topic_name, num_partitions=6, replication_factor=1):
        """
        Create a Kafka topic if it does not already exist.
        """
        try:
            # List existing topics
            existing_topics = admin_client.list_topics()
            if topic_name in existing_topics:
                print(f"Topic '{topic_name}' already exists.")
                return

            # Create the topic
            new_topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            admin_client.create_topics([new_topic])
            print(f"Topic '{topic_name}' created successfully.")
        except Exception as e:
            print(f"Failed to create topic '{topic_name}': {e}")
class YADTQ:
    def __init__(self, kafka_bootstrap_servers='localhost:9092', redis_host='localhost', redis_port=6379):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.redis_host = redis_host
        self.redis_port = redis_port
        create_topic('tasks4')
        self.task_topic = 'tasks4'
        print(self.task_topic)
        self.heartbeat_topic = 'heartbeats'
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    def track_heartbeats(self, timeout=10):
        consumer = KafkaConsumer(
        self.heartbeat_topic,
        bootstrap_servers=self.kafka_bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        worker_status = {}  # {worker_id: last_heartbeat_time}

        print("Started heartbeat monitoring...")

        while True:
            # Poll messages with a timeout (e.g., 1000ms)
            messages = consumer.poll(timeout_ms=1000)

            # Process incoming heartbeats
            for topic_partition, records in messages.items():
                for record in records:
                    # print(record.value)
                    heartbeat = record.value
                    worker_id = heartbeat['worker_id']
                    timestamp = heartbeat['timestamp']

                    # Update last heartbeat time for the worker
                    worker_status[worker_id] = timestamp
                    self.redis_client.hset('worker_status', worker_id, 'active')
                    self.redis_client.expire(f"worker:{worker_id}:heartbeat", timeout)
                    # print(worker_status)

            # Detect failed workers
            current_time = time.time()
            failed_workers = []

            for worker_id, last_heartbeat in list(worker_status.items()):
                if current_time - last_heartbeat > timeout:  # Worker timeout
                    failed_workers.append(worker_id)

            # Handle the failed workers
            for worker_id in failed_workers:
                print(f"Worker {worker_id} failed. Requeuing tasks...")
                worker_status.pop(worker_id)  # Remove from active workers
                self.redis_client.hset('worker_status', worker_id, 'failed')  # Mark worker as failed

                # Requeue tasks assigned to the failed worker
                for task_id in self.redis_client.scan_iter():
                    task_info = self.redis_client.hgetall(task_id)
                    if task_info.get('status') == 'processing' and task_info.get('worker') == worker_id:
                        # Requeue the task
                        task = {
                            'task_id': task_id,
                            'task': task_info['task'],
                            'args': json.loads(task_info['args'])
                        }
                        self.producer.send(self.task_topic, task)
                        self.redis_client.hset(task_id, mapping={'status': 'queued', 'worker': ''})
                        print(f"Task {task_id} requeued")

            # Add a small sleep to avoid busy looping
            time.sleep(1)


class YADTQClient(YADTQ):
    def __init__(self, *args, **kwargs):
        super(YADTQClient, self).__init__(*args, **kwargs)
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    def _get_partition(self, key):
        """Returns the partition number for a given key (task_id or worker_id)."""
        hash_value = hashlib.md5(key.encode('utf-8')).hexdigest()  # Hash the key
        partition = int(hash_value, 16) % 6  # Modulo with number of partitions (e.g., 6)
        return partition
    def submit_task(self, task_name, args):
        task_id = str(uuid.uuid4())
        task = {
            'task_id': task_id,
            'task': task_name,
            'args': args
        }
        partition_no=self._get_partition(task_id)
        # Store initial status in Redis
        self.redis_client.hset(task_id, mapping={'status': 'queued'})
        # Send task to Kafka
        self.producer.send(self.task_topic, task,partition=partition_no)
        self.producer.flush()
        return task_id

    def get_task_status(self, task_id):
        status_info = self.redis_client.hgetall(task_id)
        if status_info:
            return status_info
        else:
            return {'status': 'unknown'}

class YADTQWorker(YADTQ):
    def __init__(self, group_id, *args, **kwargs):
        super(YADTQWorker, self).__init__(*args, **kwargs)
        self.group_id = group_id
        self.consumer = KafkaConsumer(
            self.task_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=group_id,
            enable_auto_commit=False
        )
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.worker_id = str(uuid.uuid4())
        self.stop_event = threading.Event()

    def send_heartbeat(self):
        while not self.stop_event.is_set():
            heartbeat = {'worker_id': self.worker_id, 'timestamp': time.time()}
            self.producer.send(self.heartbeat_topic, heartbeat)
            print("heartbeat sent")
            self.producer.flush()
            time.sleep(5)  # Send heartbeat every 5 seconds

    def start_heartbeat(self):
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.start()

    def stop_heartbeat(self):
        self.stop_event.set()
        self.heartbeat_thread.join()

    def consume_tasks(self, task_functions):
        """
        task_functions: a dictionary mapping task names to functions
        """
        self.start_heartbeat()
        try:
            for message in self.consumer:
                task = message.value
                task_id = task['task_id']
                task_name = task['task']
                args = task['args']

                print(f"Worker {self.worker_id} processing task {task_id}: {task_name}({args})")

                # Update status to processing
                self.redis_client.hset(task_id, mapping={'status': 'processing'})

                try:
                    # Execute the task
                    func = task_functions.get(task_name)
                    if func:
                        result = func(*args)
                        # Update status to success and store result
                        self.redis_client.hset(task_id, mapping={'status': 'success', 'result': str(result)})
                    else:
                        # Task not found
                        self.redis_client.hset(task_id, mapping={
                            'status': 'failed',
                            'error': f'Unknown task {task_name}'
                        })
                except Exception as e:
                    # Update status to failed and store error message
                    self.redis_client.hset(task_id, mapping={'status': 'failed', 'error': str(e)})

                # Manually commit the message
                self.consumer.commit()
        except Exception as e:
            print(f"Worker encountered an error: {e}")
        finally:
            self.stop_heartbeat()
