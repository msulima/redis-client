package pl.msulima.redis.benchmark.nonblocking;

import redis.clients.jedis.Protocol;

public class Operation {

    private final byte[] command;
    private final byte[][] args;

    public static Operation get(String key) {
        return new Operation(Protocol.Command.GET, key.getBytes());
    }

    public static Operation set(String key, String value) {
        return new Operation(Protocol.Command.SET, key.getBytes(), value.getBytes());
    }

    private Operation(Protocol.Command command, byte[]... args) {
        this.command = command.raw;
        this.args = args;
    }

    public byte[] command() {
        return command;
    }

    public byte[][] args() {
        return args;
    }
}
