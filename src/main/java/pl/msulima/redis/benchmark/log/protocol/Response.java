package pl.msulima.redis.benchmark.log.protocol;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

public class Response {

    private boolean isNull;
    private byte[] bulkString;
    public String simpleString;

    private Response(boolean isNull, byte[] bulkString, String simpleString) {
        this.isNull = isNull;
        this.bulkString = bulkString;
        this.simpleString = simpleString;
    }

    public static Response simpleString(String ok) {
        return new Response(false, null, ok);
    }

    public static Response nullResponse() {
        return new Response(true, null, null);
    }

    static Response clearResponse() {
        return new Response(false, null, null);
    }

    public static Response bulkString(byte[] ok) {
        return new Response(false, ok, null);
    }

    public Response copy() {
        return new Response(isNull, bulkString, simpleString);
    }

    void setSimpleString(String simpleString) {
        this.isNull = false;
        this.bulkString = null;
        this.simpleString = simpleString;
    }

    void setNull() {
        this.isNull = true;
        this.bulkString = null;
        this.simpleString = null;
    }

    void setBulkString(byte[] bulkString) {
        this.isNull = false;
        this.bulkString = bulkString;
        this.simpleString = null;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Response.class.getSimpleName() + "[", "]")
                .add("isNull=" + isNull)
                .add("bulkString=" + Arrays.toString(bulkString))
                .add("simpleString='" + simpleString + "'")
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Response response = (Response) o;
        return isNull == response.isNull &&
                Arrays.equals(bulkString, response.bulkString) &&
                Objects.equals(simpleString, response.simpleString);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(isNull, simpleString);
        result = 31 * result + Arrays.hashCode(bulkString);
        return result;
    }
}
