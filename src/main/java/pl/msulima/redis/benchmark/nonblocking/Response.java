package pl.msulima.redis.benchmark.nonblocking;

public class Response {

    private String simpleString;

    public void setSimpleString(String simpleString) {
        this.simpleString = simpleString;
    }

    public String getSimpleString() {
        return simpleString;
    }

    public void clear() {
        simpleString = null;
    }
}
