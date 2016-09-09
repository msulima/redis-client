package pl.msulima.db;

import java.nio.ByteBuffer;
import java.util.Map;

public class Serializer {

    public static final short INT_SIZE = 4;
    public static final short KEY_SIZE = INT_SIZE;
    public static final short LONG_SIZE = 8;

    public ByteBuffer serialize(Map<Integer, Record> recordMap) {
        ByteBuffer buffer = ByteBuffer.allocate(calculateCapacity(recordMap));
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

    private int calculateCapacity(Map<Integer, Record> recordMap) {
        int size = recordMap.size();

        int headerSize = INT_SIZE + size * KEY_SIZE;
        int recordsSize = size * Record.SIZE;

        return headerSize + recordsSize;
    }

    private void putRecord(ByteBuffer buffer, Record value) {
        buffer.putInt(value.a);
        buffer.putInt(value.b);
        buffer.putLong(value.c);
    }
}
