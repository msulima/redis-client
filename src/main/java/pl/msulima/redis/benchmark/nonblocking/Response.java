package pl.msulima.redis.benchmark.nonblocking;

import com.google.common.base.MoreObjects;

import java.nio.ByteBuffer;
import java.util.Objects;

public class Response {

    private ByteBuffer bulkString;
    private boolean isNull;
    private String simpleString;

    public void clear() {
        bulkString = null;
        isNull = false;
        simpleString = null;
    }

    public Response copy() {
        Response response = new Response();
        response.bulkString = bulkString;
        response.isNull = isNull;
        response.simpleString = simpleString;
        return response;
    }

    public byte[] getBulkString() {
        return bulkString.array();
    }

    public void setBulkString(byte[] bulkString) {
        this.bulkString = ByteBuffer.wrap(bulkString);
    }

    public boolean isNull() {
        return isNull;
    }

    public void setNull(boolean aNull) {
        isNull = aNull;
    }

    public String getSimpleString() {
        return simpleString;
    }

    public void setSimpleString(String simpleString) {
        this.simpleString = simpleString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Response response = (Response) o;
        return isNull == response.isNull &&
                Objects.equals(bulkString, response.bulkString) &&
                Objects.equals(simpleString, response.simpleString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bulkString, isNull, simpleString);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("bulkString", bulkString)
                .add("isNull", isNull)
                .add("simpleString", simpleString)
                .toString();
    }

    public static Response simpleString(String ok) {
        Response response = new Response();
        response.simpleString = ok;
        return response;
    }

    public static Response bulkString(byte[] ok) {
        Response response = new Response();
        response.setBulkString(ok);
        return response;
    }
}
