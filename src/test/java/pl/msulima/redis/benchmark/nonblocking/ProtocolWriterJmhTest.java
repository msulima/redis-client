package pl.msulima.redis.benchmark.nonblocking;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import redis.clients.jedis.Protocol;

import java.nio.ByteBuffer;

@State(Scope.Thread)
public class ProtocolWriterJmhTest {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ProtocolWriterJmhTest.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .measurementTime(TimeValue.seconds(1))
                .forks(3)
                .build();

        new Runner(opt).run();
    }

    private ByteBuffer out = ByteBuffer.allocate(2048);
    private ProtocolWriter writer = new ProtocolWriter(out);

    @TearDown(Level.Invocation)
    public void clearOut() {
        out.clear();
    }

    @Benchmark
    public void testWrite() {
        writer.write(Protocol.Command.SET, new byte[100], new byte[1000], new byte[100]);
    }
}
