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

        ByteBuffer serialized = new Serializer().serialize(recordMap);
        Map<Integer, Record> deserialized = new Deserializer().deserialize(serialized);

        if (!Objects.equals(recordMap, deserialized)) {
            throw new AssertionError("should be equal");
        }
    }
}
