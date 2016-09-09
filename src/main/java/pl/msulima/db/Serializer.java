package pl.msulima.db;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Serializer {

    public static final short INT_SIZE = 4;
    public static final short KEY_SIZE = INT_SIZE;
    public static final short INDEX_SIZE = INT_SIZE;
    public static final short SIZE_SIZE = INT_SIZE;
    public static final short LONG_SIZE = 8;

    public ByteBuffer serialize(Map<Integer, List<Record>> recordMap) {
        Map<Integer, Integer> offsets = new HashMap<>(recordMap.size());

        int offset = calculateHeaderSize(recordMap.size());
        for (Map.Entry<Integer, List<Record>> entry : recordMap.entrySet()) {
            offsets.put(entry.getKey(), offset);
            offset += calculateRecordSize(entry);
        }

        ByteBuffer buffer = ByteBuffer.allocate(offset);
        buffer.putInt(recordMap.size());

        for (Map.Entry<Integer, Integer> entry : offsets.entrySet()) {
            buffer.putInt(entry.getKey());
            buffer.putInt(entry.getValue());
        }

        for (Map.Entry<Integer, List<Record>> entry : recordMap.entrySet()) {
            putRecord(buffer, entry.getValue());
        }

        buffer.flip();
        return buffer;
    }

    public int calculateHeaderSize(int size) {
        return SIZE_SIZE + size * (KEY_SIZE + INDEX_SIZE);
    }

    private int calculateRecordSize(Map.Entry<Integer, List<Record>> entry) {
        return SIZE_SIZE + entry.getValue().size() * Record.SIZE;
    }

    private void putRecord(ByteBuffer buffer, List<Record> value) {
        buffer.putInt(value.size());
        for (Record record : value) {
            putRecord(buffer, record);
        }
    }

    private void putRecord(ByteBuffer buffer, Record value) {
        buffer.putInt(value.a);
        buffer.putInt(value.b);
        buffer.putLong(value.c);
    }
}
