#!/bin/bash
set -e

sudo apt-get update
sudo apt install -y build-essential htop make ruby rubygems sysstat wget
gem install --user-install redis
wget --no-clobber http://download.redis.io/releases/redis-5.0.8.tar.gz
tar -xzf redis-5.0.8.tar.gz
pushd redis-5.0.8
make
popd
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
sudo sysctl -w net.core.rmem_max=8388608
sudo sysctl -w net.core.wmem_max=8388608
sudo sysctl -w net.core.rmem_default=65536
sudo sysctl -w net.core.wmem_default=65536
sudo sysctl -w net.ipv4.tcp_rmem='4096 87380 8388608'
sudo sysctl -w net.ipv4.tcp_wmem='4096 65536 8388608'
sudo sysctl -w net.ipv4.tcp_mem='8388608 8388608 8388608'
sudo sysctl -w net.ipv4.route.flush=1
