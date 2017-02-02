package pl.msulima.redis.benchmark.nonblocking;

import org.junit.Test;

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

        reactor.submit(Operation.set("key 1", "value 1", () -> {
        }));
        reactor.submit(Operation.get("key 1", System.out::println));

        Thread.sleep(1000);

        redis.join(10_000);
        client.join(10_000);
    }
}
