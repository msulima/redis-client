package pl.msulima.db;

import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Reader {

    private final ByteBuffer serialized;
    private final HashMap<Integer, Integer> offsets;

    public static Reader fromFile(String filename) {
        try {
            FileChannel channel = new RandomAccessFile(filename, "r").getChannel();
            MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, Math.min(channel.size(), Integer.MAX_VALUE));

            return new Reader(mappedByteBuffer);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

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
            int newPosition = offsets.get(key);
            serialized.position(newPosition);
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
