package pl.msulima.redis.benchmark.log.presentation;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.Request;
import pl.msulima.redis.benchmark.log.logbuffer.PublicationImage;
import pl.msulima.redis.benchmark.log.util.QueueUtil;

import java.util.ArrayList;
import java.util.List;

public class ReceiverAgent implements Agent {

    private final List<Receiver> clients = new ArrayList<>();
    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue;

    public ReceiverAgent(int commandQueueSize) {
        this.commandQueue = new OneToOneConcurrentArrayQueue<>(commandQueueSize);
    }

    public void registerChannelEndpoint(OneToOneConcurrentArrayQueue<Request<?>> callbacks, PublicationImage responsesImage, OneToOneConcurrentArrayQueue<Request<?>> readyResponses) {
        QueueUtil.offerOrSpin(commandQueue, () -> onRegisterChannelEndpoint(callbacks, responsesImage, readyResponses));
    }

    private void onRegisterChannelEndpoint(OneToOneConcurrentArrayQueue<Request<?>> callbacks, PublicationImage responsesImage, OneToOneConcurrentArrayQueue<Request<?>> readyResponses) {
        clients.add(new Receiver(callbacks, responsesImage, readyResponses));
    }

    @Override
    public String roleName() {
        return "receiver";
    }

    @Override
    public int doWork() {
        int totalWork = commandQueue.drain(Runnable::run);
        for (Receiver client : clients) {
            totalWork += client.doWork();
        }
        return totalWork;
    }
}
