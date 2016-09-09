package pl.msulima.db;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class Reader {

    private final ByteBuffer serialized;
    private final HashMap<Integer, Integer> keysMap;

    public Reader(ByteBuffer serialized) {
        this.serialized = serialized;

        int elementsCount = serialized.getInt();
        int headerSize = Serializer.calculateHeaderSize(elementsCount);

        this.keysMap = new HashMap<>(elementsCount);
        for (int index = 0; index < elementsCount; index++) {
            this.keysMap.put(serialized.getInt(), headerSize + index * Record.SIZE);
        }
    }

    public Record get(Integer key) {
        if (keysMap.containsKey(key)) {
            serialized.position(keysMap.get(key));
            return getRecord();
        }
        return null;
    }

    private Record getRecord() {
        int a = serialized.getInt();
        int b = serialized.getInt();
        long c = serialized.getLong();

        return new Record(a, b, c);
    }
}
