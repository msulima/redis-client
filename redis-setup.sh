#!/bin/bash

export REDIS_PRIVATE_DNS=""
export REDIS_HOST=""
export APP_HOST=""
SSH_KEY=""

ssh ec2-user@$REDIS_HOST -i $SSH_KEY
ssh ec2-user@$APP_HOST -i $SSH_KEY
sbt assembly && scp -i $SSH_KEY target/scala-2.11/redis-client-benchmark-assembly-0.4.0.jar ec2-user@$APP_HOST:/home/ec2-user
scp -i $SSH_KEY schedule.csv ec2-user@$APP_HOST:/home/ec2-user
scp -i $SSH_KEY ec2-user@$APP_HOST:/home/ec2-user/log.log .
scp -i $SSH_KEY ec2-user@$APP_HOST:/home/ec2-user/myrecording.jfr .



sudo yum -y update
sudo yum -y install make gcc ruby rubygems htop wget sysstat
gem install --user-install redis
wget http://download.redis.io/releases/redis-3.0.7.tar.gz
tar xzf redis-3.0.7.tar.gz
cd redis-3.0.7
make

src/redis-server

cd utils/create-cluster/
vi create-cluster
# change REPLICAS to 0 and 127.0.0.1 to Redis private IP
./create-cluster stop && ./create-cluster clean && ./create-cluster start && ./create-cluster create

while true; do ~/redis-3.0.7/src/redis-cli -p 30001  info | grep instantaneous_ops_per_sec; sleep 1; done

#####################

sudo yum -y update
sudo yum -y remove java-1.7.0-openjdk
sudo yum -y install git perf linux-tools-`uname -r` cmake build-essential gcc-c++ htop sysstat
sudo yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel
export JAVA_HOME=/usr/lib/jvm/java-1.8.0
java -version

# wget --no-cookies --header "Cookie: gpw_e24=xxx; oraclelicense=accept-securebackup-cookie;" "http://download.oracle.com/otn-pub/java/jdk/8u121-b13/e9e7ea248e2c4826b92b3f075a80e441/jdk-8u121-linux-x64.rpm"
# sudo rpm -i jdk-8u121-linux-x64.rpm
# export JAVA_HOME=/usr/java/jdk1.8.0_121

java -Dredis.host=$REDIS_PRIVATE_DNS -Dredis.port=30001 -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:StartFlightRecording=duration=60s,delay=30s,filename=myrecording.jfr -ms6g -mx6g -XX:+UseG1GC -Xloggc:gc.log -XX:+PrintGCDetails -XX:InitiatingHeapOccupancyPercent=30 -XX:+PreserveFramePointer -cp .:redis-client-benchmark-assembly-0.4.0.jar pl.msulima.redis.benchmark.test.TestSuite | tee -a log.log

#####################

sudo perf stat -e 'syscalls:sys_enter_*' -a sleep 10

cd ~
git clone --depth=1 https://github.com/jrudolph/perf-map-agent
cd perf-map-agent
cmake -DJAVA_INCLUDE_PATH=$JAVA_HOME/include -DJAVA_INCLUDE_PATH2=$JAVA_HOME/include/linux -DJAVA_JVM_LIBRARY=$JAVA_HOME/jre/lib/amd64/server/libjvm.so -DJAVA_AWT_LIBRARY=$JAVA_HOME/lib/amd64/libjawt.so -DJAVA_AWT_INCLUDE_PATH=$JAVA_HOME/include .
make
cd ..
sudo mv perf-map-agent /usr/lib/jvm/perf-map-agent

cd ~
git clone https://github.com/brendangregg/FlameGraph
cd FlameGraph
wget https://raw.githubusercontent.com/brendangregg/Misc/master/java/jmaps
chmod +x jmaps

sudo bash ./jmaps && sudo perf record -F 9970 -a -g -- sleep 60 && sudo perf script | ./stackcollapse-perf.pl | ./flamegraph.pl --color=java --hash > out.stacks.svg


scp -P 2222 vagrant@127.0.0.1:/home/vagrant/FlameGraph/out.stacks.svg .
scp -i $SSH_KEY ec2-user@$APP_HOST:/home/ec2-user/FlameGraph/out.stacks.svg .


# sudo strace -ttt -T -fp <PID>

sudo yum install kernel-headers-$(uname -r) perf-$(uname -r) kernel-devel-$(uname -r)

sudo apt-get install linux-headers-$(uname -r) linux-image-$(uname -r) linux-image-$(uname -r)-dbgsym

sudo perf probe -k /usr/lib/debug/boot/vmlinux-$(uname -r) --add 'tcp_sendmsg size'


###

wget https://raw.githubusercontent.com/brendangregg/perf-tools/master/syscount
chmod +x syscount


src/redis-benchmark -t set,get -n 1000000 -d 100 -P 1