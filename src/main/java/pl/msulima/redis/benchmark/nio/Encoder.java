package pl.msulima.redis.benchmark.nio;

import java.io.UnsupportedEncodingException;

public class Encoder {

    public static final String CHARSET = "UTF-8";

    public static byte[] encode(String s) {
        try {
            return s.getBytes(CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
