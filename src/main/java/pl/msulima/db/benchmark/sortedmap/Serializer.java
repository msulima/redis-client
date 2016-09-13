package pl.msulima.db.benchmark.sortedmap;

import com.google.common.base.Throwables;
import pl.msulima.db.Record;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Serializer {

    public static final short INT_SIZE = 4;
    public static final short KEY_SIZE = INT_SIZE;
    public static final short SIZE_SIZE = INT_SIZE;

    public int serialize(List<Pair> recordMap, String name) {
        try {
            Files.deleteIfExists(Paths.get(name));
            FileChannel channel = new RandomAccessFile(name, "rw").getChannel();
            int size = SIZE_SIZE + (recordMap.size() * (KEY_SIZE + Record.SIZE));
            MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
            serialize(recordMap, mappedByteBuffer);

            channel.close();

            return size;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void serialize(List<Pair> recordMap, ByteBuffer buffer) {
        buffer.putInt(recordMap.size());

        for (Pair entry : recordMap) {
            buffer.putInt(entry.getKey());
        }

        for (Pair entry : recordMap) {
            putRecord(buffer, entry.getRecord());
        }

        buffer.flip();
    }

    private void putRecord(ByteBuffer buffer, Record value) {
        buffer.putInt(value.a);
        buffer.putInt(value.b);
        buffer.putLong(value.c);
        buffer.putLong(value.d);
        buffer.putLong(value.e);
        buffer.putLong(value.f);
    }
}
