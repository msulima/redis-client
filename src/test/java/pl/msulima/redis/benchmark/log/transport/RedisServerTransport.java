package pl.msulima.redis.benchmark.log.transport;

import pl.msulima.redis.benchmark.log.Request;
import pl.msulima.redis.benchmark.log.protocol.DynamicDecoder;
import pl.msulima.redis.benchmark.log.protocol.DynamicEncoder;
import pl.msulima.redis.benchmark.log.protocol.Response;
import redis.clients.jedis.Protocol;

import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class RedisServerTransport implements Transport {

    public static final int NETWORK_BUFFER_SIZE = 16;
    private final ByteBuffer sendBuffer;
    private final ByteBuffer receiveBuffer;
    private final DynamicDecoder dynamicDecoder = new DynamicDecoder();

    private final Queue<Request<?>> requests = new LinkedList<>();

    public RedisServerTransport() {
        this.sendBuffer = ByteBuffer.allocate(NETWORK_BUFFER_SIZE);
        this.receiveBuffer = ByteBuffer.allocate(NETWORK_BUFFER_SIZE * 1024).clear();
    }

    @Override
    public void send(ByteBuffer buffer) {
        sendBuffer.clear();
        int oldLimit = buffer.limit();
        int newLimit = buffer.position() + Math.min(sendBuffer.capacity(), buffer.remaining());
        buffer.limit(newLimit);
        sendBuffer.put(buffer);
        buffer.limit(oldLimit);

        sendBuffer.flip();
        readRequests();
    }

    private void readRequests() {
        while (true) {
            dynamicDecoder.read(sendBuffer);
            Response response = dynamicDecoder.response;
            if (response.isComplete()) {
                requests.add(toCommand(response));
            } else {
                break;
            }
        }
    }

    private Request<String> toCommand(Response response) {
        Protocol.Command command = Protocol.Command.valueOf(new String(response.array[0]));
        byte[][] args = new byte[response.array.length - 1][];
        System.arraycopy(response.array, 1, args, 0, args.length);
        return new Request<>(command, Request::getSimpleString, args);
    }

    public List<Request<?>> getRequests() {
        return new ArrayList<>(requests);
    }

    public void insertSimpleStringResponse(String text) {
        receiveBuffer.put(encodeSimpleString(text));
    }

    public void insertBulkStringResponse(String text) {
        receiveBuffer.put(("$" + text.length() + "\r\n" + text + "\r\n").getBytes(DynamicEncoder.CHARSET));
    }

    private static byte[] encodeSimpleString(String ok) {
        return ("+" + ok + "\r\n").getBytes(DynamicEncoder.CHARSET);
    }

    @Override
    public void receive(ByteBuffer buffer) {
        receiveBuffer.flip();
        int originalLimit = receiveBuffer.limit();
        receiveBuffer.limit(Math.min(originalLimit, NETWORK_BUFFER_SIZE));
        buffer
                .clear()
                .put(receiveBuffer)
                .flip();
        receiveBuffer.limit(originalLimit);
        receiveBuffer.compact();
    }

    public void processRequests() {
        Request<?> poll;
        while ((poll = requests.poll()) != null) {
            if (poll.command == Protocol.Command.PING) {
                if (poll.args.length > 0) {
                    insertBulkStringResponse(new String(poll.args[0], DynamicEncoder.CHARSET));
                } else {
                    insertSimpleStringResponse("PONG");
                }
            } else {
                throw new RuntimeException("Unknown message " + poll);
            }
        }
    }

    @Override
    public void register(Selector selector, Object attachment) {
    }

    @Override
    public void connect() {
    }
}
