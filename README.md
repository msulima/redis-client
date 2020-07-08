# Redis client benchmark

Goal of this project is to check the actual limit of RPS that Redis can achieve.
For details see: [http://msulima.github.io/performance/redis/2020/07/05/requests-per-second-to-redis/](http://msulima.github.io/performance/redis/2020/07/05/requests-per-second-to-redis/).

## Setup

See `redis-setup.sh`.

To run JMH tests in IntelliJ IDEA enable annotations processing.

### Redis in docker

```shell script
for i in 6379 6380; do docker run --name redis-$i -p $i:$i -d redis --port $i --bind 0.0.0.0; done
```

### Configuring JVM assembly profiler

Based on https://metebalci.com/blog/how-to-build-the-hsdis-disassembler-plugin-on-ubuntu-18/

```shell script
sudo apt install texinfo
wget https://download.java.net/openjdk/jdk11/ri/openjdk-11+28_src.zip
# unzip
cd openjdk/src/utils/hsdis
wget https://ftp.gnu.org/gnu/binutils/binutils-2.33.1.tar.gz
tar -zxvf binutils-2.33.1.tar.gz
export BINUTILS=binutils-2.33.1
make all64
sudo cp build/linux-amd64/hsdis-amd64.so /usr/lib/jvm/java-11-openjdk-amd64/lib
```

Fix compilation error in `hsdis.c` with:
```c
app_data->dfn = disassembler(bfd_get_arch(native_bfd),
                             bfd_big_endian(native_bfd),
                             bfd_get_mach(native_bfd),
                             native_bfd);
```

### Running perf profiler

```shell script
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
export FLAMEGRAPH_DIR=/home/msulima/code/FlameGraph/
export PERF_RECORD_SECONDS=120
./gradlew installDist; build/install/redis-client/bin/redis-client &; sleep 45; ../perf-map-agent/bin/perf-java-flames $(ps aux | grep PreserveFramePointer | grep -v grep | awk '{ print $2 }')
```
