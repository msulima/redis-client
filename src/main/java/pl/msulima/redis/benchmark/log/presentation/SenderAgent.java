package pl.msulima.redis.benchmark.log.presentation;

import org.agrona.BufferUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.Request;
import pl.msulima.redis.benchmark.log.protocol.DynamicEncoder;
import pl.msulima.redis.benchmark.log.util.QueueUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class SenderAgent implements Agent {

    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue;
    private final int bufferSize;
    private final List<SenderData> clients = new ArrayList<>();

    public SenderAgent(int bufferSize, int commandQueueSize) {
        this.commandQueue = new OneToOneConcurrentArrayQueue<>(commandQueueSize);
        this.bufferSize = bufferSize;
    }

    public void registerChannelEndpoint(Queue<Request<?>> requests, OneToOneConcurrentArrayQueue<byte[]> serializedRequests, Queue<Request<?>> callbacks) {
        QueueUtil.offerOrSpin(commandQueue, () -> onRegisterChannelEndpoint(requests, serializedRequests, callbacks));
    }

    private void onRegisterChannelEndpoint(Queue<Request<?>> requests, OneToOneConcurrentArrayQueue<byte[]> serializedRequests, Queue<Request<?>> callbacks) {
        clients.add(new SenderData(requests, serializedRequests, callbacks, bufferSize));
    }

    @Override
    public String roleName() {
        return "senderAgent";
    }

    @Override
    public int doWork() {
        int totalWork = commandQueue.drain(Runnable::run);
        for (SenderData client : clients) {
            totalWork += client.doWork();
        }
        return totalWork;
    }


    private static final class SenderData {

        private final DynamicEncoder encoder = new DynamicEncoder();
        private final Queue<Request<?>> requests;
        private final OneToOneConcurrentArrayQueue<byte[]> serializedRequests;
        private final Queue<Request<?>> callbacks;
        private final ByteBuffer buffer;

        private SenderData(Queue<Request<?>> requests, OneToOneConcurrentArrayQueue<byte[]> serializedRequests, Queue<Request<?>> callbacks, int bufferSize) {
            this.requests = requests;
            this.serializedRequests = serializedRequests;
            this.callbacks = callbacks;
            this.buffer = BufferUtil.allocateDirectAligned(bufferSize, 64);
        }

        private int doWork() {
            buffer.clear();
            while (encoder.write(buffer)) {
                Request<?> request = poll();
                if (request != null) {
                    encoder.setRequest(request.command, request.args);
                } else {
                    break;
                }
            }
            buffer.flip();
            int remaining = buffer.remaining();
            if (remaining >= 0) {
                byte[] bytes = new byte[remaining];
                buffer.get(bytes);
                serializedRequests.add(bytes);
            }
            return remaining;
        }

        private Request<?> poll() {
            Request<?> request = requests.peek();
            if (request != null) {
                if (callbacks.offer(request)) {
                    requests.remove();
                    // TODO  what if send fails?
                    return request;
                }
            }
            return null;
        }
    }
}
