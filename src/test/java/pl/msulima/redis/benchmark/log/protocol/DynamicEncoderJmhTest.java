package pl.msulima.redis.benchmark.log.protocol;

import org.agrona.BufferUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.nio.ByteBuffer;

@State(Scope.Thread)
public class DynamicEncoderJmhTest {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(DynamicEncoderJmhTest.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .measurementTime(TimeValue.seconds(10))
                .jvmArgs("-Dagrona.disable.bounds.checks=true")
                .addProfiler("perfasm")
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    private ByteBuffer out = BufferUtil.allocateDirectAligned(2048, 64);
    private DynamicEncoder writer = new DynamicEncoder();
    private UnsafeDynamicEncoder unsafeWriter = new UnsafeDynamicEncoder();
    @Param({"6"})
    private int arg1Size;
    @Param({"100"})
    private int arg2Size;

    @Setup(Level.Invocation)
    public void setup() {
        byte[][] args = new byte[][]{new byte[arg1Size], new byte[arg2Size]};
        writer.setRequest(Command.SET, args);
        unsafeWriter.setRequest(Command.SET, args);
    }

    @TearDown(Level.Invocation)
    public void clearOut() {
        out.clear();
    }

    @Benchmark
    public boolean testWrite() {
        return writer.write(out);
    }

    @Benchmark
    public boolean testUnsafeWrite() {
        return unsafeWriter.write(out);
    }
}
