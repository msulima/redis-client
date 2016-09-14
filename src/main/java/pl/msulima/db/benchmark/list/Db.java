package pl.msulima.db.benchmark.list;

import com.google.common.collect.Lists;
import pl.msulima.db.Record;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Db {

    public static void main(String... args) {
        Map<Integer, List<Record>> recordMap = new HashMap<>();
        recordMap.put(4, Lists.newArrayList(
                new Record(5, 6, 7L, 0, 0, 0)
        ));
        recordMap.put(Integer.MAX_VALUE, Lists.newArrayList(
                new Record(Integer.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, 0, 0, 0),
                new Record(Integer.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE, 0, 0, 0)
        ));

        ByteBuffer serialized = new Serializer().serialize(recordMap);

        Reader reader = new Reader(serialized);
        Map<Integer, List<Record>> deserialized = new HashMap<>();
        deserialized.put(4, Lists.newArrayList(reader.get(4)));
        deserialized.put(Integer.MAX_VALUE, reader.get(Integer.MAX_VALUE));

        System.out.println(recordMap);
        System.out.println(deserialized);

        if (!Objects.equals(recordMap, deserialized)) {
            throw new AssertionError("should be equal");
        }
    }
}