package pl.msulima.redis.benchmark.nonblocking;

import com.google.common.base.MoreObjects;

import java.util.Objects;

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

    public static Response string(String string) {
        Response response = new Response();
        response.setString(string);
        return response;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("readString", readString)
                .add("isNull", isNull)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Response response = (Response) o;
        return isNull == response.isNull &&
                Objects.equals(readString, response.readString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(readString, isNull);
    }

    public Response copy() {
        Response response = new Response();
        response.setIsNull(isNull);
        response.setString(readString);
        return response;
    }
}
