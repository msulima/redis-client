package pl.msulima.redis.benchmark.nonblocking;

public class Response {

    private String readString;
    private boolean isNull;

    public void setString(String simpleString) {
        this.readString = simpleString;
    }

    public String getString() {
        return readString;
    }

    public void clear() {
        readString = null;
    }

    public void setIsNull(boolean isNull) {
        this.isNull = isNull;
    }

    public boolean isNull() {
        return isNull;
    }
}
