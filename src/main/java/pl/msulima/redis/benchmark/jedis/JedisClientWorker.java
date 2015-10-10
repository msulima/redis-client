package pl.msulima.redis.benchmark.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.List;

public class JedisClientWorker implements Runnable {

    private static final JedisPool pool = new JedisPool("localhost");
    private final List<Operation> requests;

    public JedisClientWorker(List<Operation> requests) {
        this.requests = requests;
    }

    @Override
    public void run() {
        try (Jedis jedis = pool.getResource()) {
            Pipeline pipeline = jedis.pipelined();
            for (Operation request : requests) {
                request.run(pipeline);
            }
            pipeline.sync();
            requests.forEach(Operation::done);
        }
    }
}
