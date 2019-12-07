package pl.msulima.redis.benchmark.log;

import pl.msulima.redis.benchmark.log.protocol.Response;

import java.util.ArrayList;
import java.util.List;

public class ReceiveChannelEndpoint {

    public final List<Response> responses = new ArrayList<>();

    public void onResponse(Response response) {
        responses.add(response.copy());
    }
}
