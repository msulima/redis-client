#!/bin/bash

REDIS_HOST=
APP_HOST=

sudo apt-get update
sudo apt-get install make gcc ruby htop --yes
gem install --user-install redis
wget http://download.redis.io/releases/redis-3.0.7.tar.gz
tar xzf redis-3.0.7.tar.gz
cd redis-3.0.7

make

src/redis-server

cd utils/create-cluster/
./create-cluster stop && ./create-cluster clean && ./create-cluster start && ./create-cluster create

while true; do ~/redis-3.0.7/src/redis-cli -p 30001  info | grep instantaneous_ops_per_sec; sleep 1; done

#####################

sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer git linux-tools-`uname -r` cmake build-essential --yes
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

java -ms5g -XX:+UseG1GC -Xloggc:gc.log -XX:+PrintGCDetails -XX:InitiatingHeapOccupancyPercent=30 -XX:+PreserveFramePointer -Dredis.host=$REDIS_HOST -Dredis.port=6379 -cp .:redis-client-benchmark-assembly-0.3.0.jar pl.msulima.redis.benchmark.test.TestSuite | tee -a log.log


git clone --depth=1 https://github.com/jrudolph/perf-map-agent
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
cd perf-map-agent
cmake .
make
cd ..
sudo mv perf-map-agent /usr/lib/jvm/perf-map-agent

cd ~
git clone https://github.com/brendangregg/FlameGraph
cd FlameGraph
wget https://raw.githubusercontent.com/brendangregg/Misc/master/java/jmaps
chmod +x jmaps

sudo bash ./jmaps && sudo perf record -F 999 -a -g -- sleep 60 && sudo perf script | ./stackcollapse-perf.pl | ./flamegraph.pl --color=java --hash > out.stacks.svg


scp -P 2222 vagrant@127.0.0.1:/home/vagrant/FlameGraph/out.stacks.svg .


sudo strace -ttt -T -fp <PID>
