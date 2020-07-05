#!/bin/bash
set -e

sudo apt-get update
sudo apt-get install -y build-essential cmake git htop linux-headers-$(uname -r) linux-tools-$(uname -r) linux-tools-common openjdk-11-source sysstat
if [ ! -d perf-map-agent ]; then
  git clone --depth=1 https://github.com/jvm-profiling-tools/perf-map-agent.git
fi
pushd perf-map-agent
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 cmake .
make
popd
if [ ! -d FlameGraph ]; then
  git clone --depth=1 https://github.com/brendangregg/FlameGraph
fi
