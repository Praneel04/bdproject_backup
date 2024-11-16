# RR-Team-52-yadtq-yet-another-distributed-task-queue-

Commit-1 creates a simple simulation for 1 worker where the client submits a simple addition task. The worker performs the said tasks in a few seconds and client's query is returned as a successful response from the redis backend.
Week 2:
Decoupled the kafka broker from the workers and clients. Added support for multiple clients and workers. Added functionality for heartbeats for workers.
