package pl.msulima.db.benchmark.sortedmap;

import com.google.common.base.Throwables;
import pl.msulima.db.Record;
import uk.co.real_logic.agrona.collections.Int2IntHashMap;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Reader {

    public static final int MISSING_VALUE = -1;
    private final ByteBuffer serialized;
    private final Int2IntHashMap offsets;

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
        this.offsets = new Int2IntHashMap(elementsCount, 0.999999, MISSING_VALUE);

        int offset = Serializer.SIZE_SIZE + elementsCount * Serializer.KEY_SIZE;
        for (int index = 0; index < elementsCount; index++) {
            int key = serialized.getInt();
            this.offsets.put(key, offset);
            offset += Record.SIZE;
        }
    }

    public Record get(int key) {
        if (offsets.containsKey(key)) {
            int newPosition = offsets.get(key);
            serialized.position(newPosition);
            return getRecord();
        }
        return null;
    }

    private Record getRecord() {
        int a = serialized.getInt();
        int b = serialized.getInt();
        long c = serialized.getLong();
        long d = serialized.getLong();
        long e = serialized.getLong();
        long f = serialized.getLong();

        return new Record(a, b, c, d, e, f);
    }
}
