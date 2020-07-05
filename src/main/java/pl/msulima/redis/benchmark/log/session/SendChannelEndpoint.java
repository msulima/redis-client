package pl.msulima.redis.benchmark.log.session;

import pl.msulima.redis.benchmark.log.logbuffer.PublicationImage;
import pl.msulima.redis.benchmark.log.transport.Transport;

import java.nio.ByteBuffer;

public class SendChannelEndpoint {

    private final PublicationImage image;
    private ByteBuffer lastBuffer = ByteBuffer.allocate(0);

    public SendChannelEndpoint(PublicationImage image) {
        this.image = image;
    }

    int send(Transport transport) {
        int position = lastBuffer.remaining();
        transport.send(lastBuffer);
        if (lastBuffer.hasRemaining()) {
            return position - lastBuffer.remaining();
        } else {
            image.commitRead(lastBuffer);
        }

        lastBuffer = image.readClaim();
        int startPosition = lastBuffer.position();
        transport.send(lastBuffer);
        return lastBuffer.position() - startPosition;
    }
}
