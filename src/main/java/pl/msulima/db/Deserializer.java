package pl.msulima.db;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Deserializer {

    public Map<Integer, Record> deserialize(ByteBuffer serialized) {
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

    private Record getRecord(ByteBuffer serialized) {
        int a = serialized.getInt();
        int b = serialized.getInt();
        long c = serialized.getLong();

        return new Record(a, b, c);
    }
}
