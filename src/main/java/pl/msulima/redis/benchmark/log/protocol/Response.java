package pl.msulima.redis.benchmark.log.protocol;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

public class Response {

    private boolean isNull;
    public byte[] bulkString;
    public String simpleString;
    public byte[][] array;

    private Response(boolean isNull, byte[] bulkString, String simpleString, byte[][] array) {
        this.isNull = isNull;
        this.bulkString = bulkString;
        this.simpleString = simpleString;
        this.array = array;
    }

    public static Response simpleString(String data) {
        return new Response(false, null, data, null);
    }

    public static Response nullResponse() {
        return new Response(true, null, null, null);
    }

    static Response clearResponse() {
        return new Response(false, null, null, null);
    }

    public static Response bulkString(byte[] data) {
        return new Response(false, data, null, null);
    }

    public Response copy() {
        return new Response(isNull, bulkString, simpleString, array);
    }

    public void clear() {
        this.isNull = false;
        this.bulkString = null;
        this.simpleString = null;
        this.array = null;
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

    void setArray(byte[][] array) {
        this.isNull = false;
        this.bulkString = null;
        this.simpleString = null;
        this.array = array;
    }

    public boolean isComplete() {
        return isNull || simpleString != null || bulkString != null || array != null;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Response.class.getSimpleName() + "[", "]")
                .add("isNull=" + isNull)
                .add("bulkString=" + Arrays.toString(bulkString))
                .add("simpleString='" + simpleString + "'")
                .add("array=" + Arrays.toString(array))
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Response response = (Response) o;
        return isNull == response.isNull &&
                Arrays.equals(bulkString, response.bulkString) &&
                Objects.equals(simpleString, response.simpleString) &&
                Arrays.equals(array, response.array);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(isNull, simpleString);
        result = 31 * result + Arrays.hashCode(bulkString);
        result = 31 * result + Arrays.hashCode(array);
        return result;
    }
}
