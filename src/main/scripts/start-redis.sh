#!/bin/bash
set -e

redis-6.0.5/src/redis-server --save "" --appendonly no --bind 0.0.0.0 --port "${1:-6379}"
