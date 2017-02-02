package pl.msulima.redis.benchmark.nonblocking;

public class Response {

    private String readString;

    public void setString(String simpleString) {
        this.readString = simpleString;
    }

    public String getString() {
        return readString;
    }

    public void clear() {
        readString = null;
    }
}
