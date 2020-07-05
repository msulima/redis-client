package pl.msulima.redis.benchmark.log.presentation;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.Request;
import pl.msulima.redis.benchmark.log.logbuffer.PublicationImage;
import pl.msulima.redis.benchmark.log.util.QueueUtil;

import java.util.ArrayList;
import java.util.List;

public class SenderAgent implements Agent {

    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue;
    private final List<Sender> clients = new ArrayList<>();

    public SenderAgent(int commandQueueSize) {
        this.commandQueue = new OneToOneConcurrentArrayQueue<>(commandQueueSize);
    }

    public void registerChannelEndpoint(ManyToOneConcurrentArrayQueue<Request<?>> requests, OneToOneConcurrentArrayQueue<Request<?>> callbacks, PublicationImage requestsImage) {
        QueueUtil.offerOrSpin(commandQueue, () -> onRegisterChannelEndpoint(requests, callbacks, requestsImage));
    }

    private void onRegisterChannelEndpoint(ManyToOneConcurrentArrayQueue<Request<?>> requests, OneToOneConcurrentArrayQueue<Request<?>> callbacks, PublicationImage requestsImage) {
        clients.add(new Sender(requests, callbacks, requestsImage));
    }

    @Override
    public String roleName() {
        return "sender";
    }

    @Override
    public int doWork() {
        int totalWork = commandQueue.drain(Runnable::run);
        for (Sender client : clients) {
            totalWork += client.doWork();
        }
        return totalWork;
    }
}
