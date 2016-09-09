package pl.msulima.db;

import com.google.common.base.MoreObjects;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Serializer {

    public static final short INT_SIZE = 4;
    public static final short KEY_SIZE = INT_SIZE;
    public static final short LONG_SIZE = 8;

    public static void main(String... args) {
        Map<Integer, Record> recordMap = new HashMap<>();
        recordMap.put(4, new Record(5, 6, 7L));
        recordMap.put(Integer.MAX_VALUE, new Record(Integer.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE));

        ByteBuffer serialized = serialize(recordMap);
        Map<Integer, Record> deserialized = deserialize(serialized);

        if (!Objects.equals(recordMap, deserialized)) {
            throw new AssertionError("should be equal");
        }
    }

    public static ByteBuffer serialize(Map<Integer, Record> recordMap) {
        ByteBuffer buffer = ByteBuffer.allocate(getSize(recordMap));
        buffer.putInt(recordMap.size());

        for (Map.Entry<Integer, Record> entry : recordMap.entrySet()) {
            buffer.putInt(entry.getKey());
        }
        for (Map.Entry<Integer, Record> entry : recordMap.entrySet()) {
            putRecord(buffer, entry.getValue());
        }

        buffer.flip();
        return buffer;
    }

    private static int getSize(Map<Integer, Record> recordMap) {
        int size = recordMap.size();

        int headerSize = INT_SIZE + size * KEY_SIZE;
        int recordsSize = size * Record.SIZE;

        return headerSize + recordsSize;
    }

    private static void putRecord(ByteBuffer buffer, Record value) {
        buffer.putInt(value.a);
        buffer.putInt(value.b);
        buffer.putLong(value.c);
    }

    public static Map<Integer, Record> deserialize(ByteBuffer serialized) {
        int size = serialized.getInt();

        int[] indices = new int[size];
        for (int index = 0; index < size; index++) {
            indices[index] = serialized.getInt();
        }

        HashMap<Integer, Record> recordsMap = new HashMap<>(size);
        for (int index = 0; index < size; index++) {
            recordsMap.put(indices[index], getRecord(serialized));
        }

        return recordsMap;
    }

    private static Record getRecord(ByteBuffer serialized) {
        int a = serialized.getInt();
        int b = serialized.getInt();
        long c = serialized.getLong();

        return new Record(a, b, c);
    }
}

class Record {

    public static final short SIZE = Serializer.INT_SIZE * 2 + Serializer.LONG_SIZE;

    public final int a;
    public final int b;
    public final long c;

    Record(int a, int b, long c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return a == record.a &&
                b == record.b &&
                c == record.c;
    }

    @Override
    public int hashCode() {
        return Objects.hash(a, b, c);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("a", a)
                .add("b", b)
                .add("c", c)
                .toString();
    }
}
