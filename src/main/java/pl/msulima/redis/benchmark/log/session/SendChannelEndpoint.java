package pl.msulima.redis.benchmark.log.session;

import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.transport.Transport;

import java.nio.ByteBuffer;

public class SendChannelEndpoint {

    private final OneToOneConcurrentArrayQueue<byte[]> requests;
    private ByteBuffer lastBuffer = ByteBuffer.allocate(0);

    public SendChannelEndpoint(OneToOneConcurrentArrayQueue<byte[]> requests) {
        this.requests = requests;
    }

    int send(Transport transport) {
        int position = lastBuffer.remaining();
        transport.send(lastBuffer);
        if (lastBuffer.hasRemaining()) {
            return position - lastBuffer.remaining();
        }

        int workDone = 0;
        byte[] bytes;
        while ((bytes = requests.poll()) != null) {
            lastBuffer = ByteBuffer.wrap(bytes);
            workDone += lastBuffer.remaining();
            transport.send(lastBuffer);
            if (lastBuffer.hasRemaining()) {
                break;
            }
        }
        return workDone;
    }
}
