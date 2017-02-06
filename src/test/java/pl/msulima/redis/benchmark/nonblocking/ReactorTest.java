package pl.msulima.redis.benchmark.nonblocking;

import com.google.common.base.Charsets;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactorTest {

    @Test
    public void test() throws InterruptedException {
        Thread redis = new Thread(new RedisStub());
        redis.setName("Redis");
        redis.start();

        Reactor reactor = new Reactor(RedisStub.PORT);
        Thread client = new Thread(reactor);
        client.setName("Client");
        client.start();

        CountDownLatch latch = new CountDownLatch(1);
        List<String> responses = Collections.synchronizedList(new ArrayList<>());
        reactor.set("key 1".getBytes(), "value 1".getBytes(), () -> {
        });
        reactor.get("key 1".getBytes(), (e) -> {
            responses.add(new String(e, Charsets.US_ASCII));
            reactor.set("FINISH".getBytes(), "".getBytes(), latch::countDown);
        });

        latch.await(10, TimeUnit.SECONDS);

        redis.interrupt();
        client.interrupt();

        redis.join(10_000);
        client.join(10_000);

        assertThat(responses).containsExactly("value 1");
    }
}
