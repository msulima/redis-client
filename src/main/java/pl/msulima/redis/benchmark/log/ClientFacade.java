package pl.msulima.redis.benchmark.log;

import pl.msulima.redis.benchmark.log.protocol.Response;
import redis.clients.jedis.Protocol;

import java.util.concurrent.CompletionStage;

public class ClientFacade {

    private final Driver driver;

    public ClientFacade(Driver driver) {
        this.driver = driver;
    }

    public CompletionStage<String> ping() {
        return driver.offer(Protocol.Command.PING, ClientFacade::decodeSimpleString);
    }

    public CompletionStage<String> set(byte[] key, byte[] value) {
        return driver.offer(Protocol.Command.SET, ClientFacade::decodeSimpleString, key, value);
    }

    private static String decodeSimpleString(Response response) {
        return response.simpleString;
    }
}
