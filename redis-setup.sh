#!/bin/bash

REDIS_HOST=
APP_HOST=

sudo apt-get update
sudo apt-get install make gcc ruby htop --yes
wget http://download.redis.io/releases/redis-3.0.7.tar.gz
tar xzf redis-3.0.7.tar.gz
cd redis-3.0.7
make

src/redis-server


gem install --user-install redis

cd utils/create-cluster/
./create-cluster stop && ./create-cluster clean && ./create-cluster start && ./create-cluster create

sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer --yes
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

# -XX:+PreserveFramePointer
java -ms10g -XX:+UseG1GC -Xloggc:gc.log -XX:+PrintGCDetails -Dredis.host=$REDIS_HOST -Dredis.port=6379 -cp .:redis-client-benchmark-assembly-0.2.0.jar pl.msulima.redis.benchmark.test.TestSuite | tee -a log.log

sudo apt-get install git linux-tools-3.13.0-62-generic cmake build-essential --yes

git clone --depth=1 https://github.com/jrudolph/perf-map-agent
cd perf-map-agent
cmake .
make
cd ..
sudo mv perf-map-agent /usr/lib/jvm/perf-map-agent


sudo chown root /tmp/perf-*.map
sudo perf script | ./stackcollapse-perf.pl | ./flamegraph.pl --color=java --hash > flamegraph.svg



cd ~
git clone https://github.com/brendangregg/FlameGraph
cd FlameGraph
wget https://raw.githubusercontent.com/brendangregg/Misc/master/java/jmaps
chmod +x jmaps
sudo bash ./jmaps

sudo perf record -F 99 -a -g -- sleep 30
sudo perf script > out.stacks
./stackcollapse-perf.pl out.stacks | ./flamegraph.pl --color=java --hash > out.stacks.svg

sudo perf record -F 99 -a -g -- sleep 60 && sudo perf script | ./stackcollapse-perf.pl > out.perf-folded && ./flamegraph.pl out.perf-folded > perf-kernel.svg




scp -P 2222 vagrant@127.0.0.1:/home/vagrant/FlameGraph/out.stacks.svg .


sudo strace -ttt -T -fp <PID>
