#!/bin/bash

export REDIS_PRIVATE_DNS=""
export REDIS_HOST=""
export APP_HOST=""
SSH_KEY=""

scp -i $SSH_KEY src/main/scripts/install-java.sh ubuntu@$APP_HOST:/home/ubuntu
scp -i $SSH_KEY src/main/scripts/install-redis.sh ubuntu@$REDIS_HOST:/home/ubuntu
scp -i $SSH_KEY src/main/scripts/start-redis.sh ubuntu@$REDIS_HOST:/home/ubuntu
ssh -i $SSH_KEY ubuntu@$APP_HOST /home/ubuntu/install-java.sh
ssh -i $SSH_KEY ubuntu@$REDIS_HOST /home/ubuntu/install-redis.sh

./gradlew build -xtest
scp -i $SSH_KEY build/distributions/redis-client.tar ubuntu@$APP_HOST:/home/ubuntu
ssh -i $SSH_KEY ubuntu@$APP_HOST tar -xvf redis-client.tar
scp -i $SSH_KEY schedule.csv ubuntu@$APP_HOST:/home/ubuntu

JAVA_OPTS=-Dredis.host=$REDIS_PRIVATE_DNS redis-client/bin/redis-client

#####################

src/redis-server

cd utils/create-cluster/
vi create-cluster
# change REPLICAS to 0 and 127.0.0.1 to Redis private IP
./create-cluster stop && ./create-cluster clean && ./create-cluster start && ./create-cluster create

while true; do ~/redis-6.0.5/src/redis-cli -p 30001  info | grep instantaneous_ops_per_sec; sleep 1; done

#####################

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