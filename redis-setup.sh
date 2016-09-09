#!/bin/bash

REDIS_PRIVATE_DNS=ip-172-31-43-131.eu-west-1.compute.internal
REDIS_HOST=ec2-52-30-132-120.eu-west-1.compute.amazonaws.com
APP_HOST=ec2-52-50-6-153.eu-west-1.compute.amazonaws.com
SSH_KEY=~/.ssh/msulima-eu-west.pem

ssh ec2-user@$REDIS_HOST -i $SSH_KEY
ssh ec2-user@$APP_HOST -i $SSH_KEY
sbt assembly && scp -i $SSH_KEY target/scala-2.11/redis-client-benchmark-assembly-0.3.0.jar ec2-user@$APP_HOST:/home/ec2-user
scp -i $SSH_KEY schedule.csv ec2-user@$APP_HOST:/home/ec2-user



sudo yum -y update
sudo yum -y install make gcc ruby rubygems htop
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

sudo yum -y update
sudo yum -y remove java-1.7.0-openjdk
sudo yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel git perf linux-tools-`uname -r` cmake build-essential gcc-c++
export JAVA_HOME=/usr/lib/jvm/java-1.8.0

java -mx5g -XX:+UseG1GC -Xloggc:gc.log -XX:+PrintGCDetails -XX:InitiatingHeapOccupancyPercent=30 -XX:+PreserveFramePointer -Dredis.host=$REDIS_PRIVATE_DNS -Dredis.port=30001 -cp .:redis-client-benchmark-assembly-0.3.0.jar pl.msulima.redis.benchmark.test.TestSuite | tee -a log.log


cd ~
git clone --depth=1 https://github.com/jrudolph/perf-map-agent
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



sudo apt-get install linux-headers-3.13.0-79-generic linux-image-3.13.0-79-generic linux-image-3.13.0-79-generic-dbgsym

sudo perf probe -k /usr/lib/debug/boot/vmlinux-$(uname -r) --add 'tcp_sendmsg size'
