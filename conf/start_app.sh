#!/bin/bash

scheduler_pid=$(pgrep -f "dask-scheduler")
[ -z "$scheduler_pid" ] && dask scheduler --port 8786 & sleep 5

worker_pid=$(pgrep -f "dask-worker tcp://localhost:8786")
[ -z "$worker_pid" ] && dask worker tcp://localhost:8786 --memory-limit 8GB & sleep 5

gunicorn woa23_app:app -w 2 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:8050 --keyfile conf/privkey.pem --certfile conf/fullchain.pem --timeout 120 --reload
