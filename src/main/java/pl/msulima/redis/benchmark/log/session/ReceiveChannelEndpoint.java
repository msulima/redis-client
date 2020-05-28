package pl.msulima.redis.benchmark.log.session;

import pl.msulima.redis.benchmark.log.logbuffer.PublicationImage;
import pl.msulima.redis.benchmark.log.transport.Transport;

import java.nio.ByteBuffer;

public class ReceiveChannelEndpoint {

    private final PublicationImage image;

    public ReceiveChannelEndpoint(PublicationImage image) {
        this.image = image;
    }

    int receive(Transport transport) {
        ByteBuffer buffer = image.writeClaim();
        int position = buffer.remaining();
        transport.receive(buffer);
        image.commitWrite(buffer);
        return buffer.remaining() - position;
    }
}
