package pl.msulima.db;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Db {

    public static void main(String... args) {
        Map<Integer, Record> recordMap = new HashMap<>();
        recordMap.put(4, new Record(5, 6, 7L));
        recordMap.put(Integer.MAX_VALUE, new Record(Integer.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE));
        recordMap.put(Integer.MIN_VALUE, new Record(Integer.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE));

        ByteBuffer serialized = new Serializer().serialize(recordMap);

        Reader reader = new Reader(serialized);
        Map<Integer, Record> deserialized = new HashMap<>();
        deserialized.put(4, reader.get(4));
        deserialized.put(Integer.MAX_VALUE, reader.get(Integer.MAX_VALUE));
        deserialized.put(Integer.MIN_VALUE, reader.get(Integer.MIN_VALUE));

        System.out.println(recordMap);
        System.out.println(deserialized);

        if (!Objects.equals(recordMap, deserialized)) {
            throw new AssertionError("should be equal");
        }
    }
}
