#!/bin/sh
set -e

# Starts uvicorn in the background, then execs nginx in the foreground as
# PID 1 -- simplest way to run both processes in one container without a
# full process supervisor at this scale.
uvicorn main:app --host 127.0.0.1 --port 8000 &

exec nginx -g "daemon off;"
