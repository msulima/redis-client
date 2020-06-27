#!/bin/bash
set -e

redis-5.0.8/src/redis-server --save "" --appendonly no --bind 0.0.0.0 --port ${1:-6379}
