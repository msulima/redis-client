package pl.msulima.redis.benchmark.log.presentation;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.Request;
import pl.msulima.redis.benchmark.log.protocol.DynamicDecoder;
import pl.msulima.redis.benchmark.log.protocol.Response;
import pl.msulima.redis.benchmark.log.util.QueueUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ReceiverAgent implements Agent {

    private final List<ReceiverData> clients = new ArrayList<>();
    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue;

    public ReceiverAgent(int commandQueueSize) {
        this.commandQueue = new OneToOneConcurrentArrayQueue<>(commandQueueSize);
    }

    public void registerChannelEndpoint(OneToOneConcurrentArrayQueue<byte[]> responses, OneToOneConcurrentArrayQueue<Request<?>> callbacks) {
        QueueUtil.offerOrSpin(commandQueue, () -> onRegisterChannelEndpoint(responses, callbacks));
    }

    private void onRegisterChannelEndpoint(OneToOneConcurrentArrayQueue<byte[]> responses, OneToOneConcurrentArrayQueue<Request<?>> callbacks) {
        clients.add(new ReceiverData(responses, callbacks));
    }

    @Override
    public String roleName() {
        return "receiverAgent";
    }

    @Override
    public int doWork() {
        int totalWork = commandQueue.drain(Runnable::run);
        for (ReceiverData client : clients) {
            totalWork += runClient(client);
        }
        return totalWork;
    }

    private int runClient(ReceiverData client) {
        byte[] bytes = client.responses.poll();
        if (bytes == null) {
            return 0;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (true) {
            client.decoder.read(buffer);
            Response response = client.decoder.response;
            if (response.isComplete()) {
                onResponse(client, response);
            } else {
                break;
            }
        }
        return buffer.position();
    }

    private void onResponse(ReceiverData client, Response response) {
        Request<?> request = client.callbacks.poll();
        if (request == null) {
            throw new IllegalStateException("Got response for unknown request " + response);
        }
        request.complete(response);
    }

    private static final class ReceiverData {

        private final DynamicDecoder decoder = new DynamicDecoder();
        private final OneToOneConcurrentArrayQueue<byte[]> responses;
        private final OneToOneConcurrentArrayQueue<Request<?>> callbacks;

        public ReceiverData(OneToOneConcurrentArrayQueue<byte[]> responses, OneToOneConcurrentArrayQueue<Request<?>> callbacks) {
            this.responses = responses;
            this.callbacks = callbacks;
        }
    }
}
