package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.util.QueueUtil;

import java.util.ArrayList;
import java.util.List;

public class FinisherAgent implements Agent {

    private static final int RESPONSES_BUFFER_SIZE = 1024;

    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue;
    private final List<OneToOneConcurrentArrayQueue<Request<?>>> responses = new ArrayList<>();
    private final List<Request<?>> responsesBuffer = new ArrayList<>(RESPONSES_BUFFER_SIZE);

    public FinisherAgent(int commandQueueSize) {
        this.commandQueue = new OneToOneConcurrentArrayQueue<>(commandQueueSize);
    }

    public void registerChannelEndpoint(OneToOneConcurrentArrayQueue<Request<?>> callbacks) {
        QueueUtil.offerOrSpin(commandQueue, () -> onRegisterChannelEndpoint(callbacks));
    }

    private void onRegisterChannelEndpoint(OneToOneConcurrentArrayQueue<Request<?>> callbacks) {
        responses.add(callbacks);
    }

    @Override
    public String roleName() {
        return "finisher";
    }

    @Override
    public int doWork() {
        int totalWork = commandQueue.drain(Runnable::run);
        for (OneToOneConcurrentArrayQueue<Request<?>> callbacks : responses) {
            callbacks.drainTo(responsesBuffer, RESPONSES_BUFFER_SIZE);
            totalWork += responsesBuffer.size();
            responsesBuffer.forEach(Request::fireCallback);
            responsesBuffer.clear();
        }
        return totalWork;
    }
}
