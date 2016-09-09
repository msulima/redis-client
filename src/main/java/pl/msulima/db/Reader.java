package pl.msulima.db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Reader {

    private final ByteBuffer serialized;
    private final HashMap<Integer, Integer> offsets;

    public Reader(ByteBuffer serialized) {
        this.serialized = serialized;

        int elementsCount = serialized.getInt();
        this.offsets = new HashMap<>(elementsCount);

        for (int index = 0; index < elementsCount; index++) {
            int key = serialized.getInt();
            int value = serialized.getInt();
            this.offsets.put(key, value);
        }
    }

    public List<Record> get(Integer key) {
        if (offsets.containsKey(key)) {
            serialized.position(offsets.get(key));
            return getRecords();
        }
        return null;
    }

    private List<Record> getRecords() {
        int size = serialized.getInt();
        List<Record> records = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            records.add(getRecord());
        }

        return records;
    }

    private Record getRecord() {
        int a = serialized.getInt();
        int b = serialized.getInt();
        long c = serialized.getLong();

        return new Record(a, b, c);
    }
}
