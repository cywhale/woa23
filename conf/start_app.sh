#!/bin/bash

gunicorn woa23_app:app -w 2 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:8050 --keyfile conf/privkey.pem --certfile conf/fullchain.pem --timeout 120 --reload
