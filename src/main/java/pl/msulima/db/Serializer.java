package pl.msulima.db;

import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class Serializer {

    public static final short INT_SIZE = 4;
    public static final short KEY_SIZE = INT_SIZE;
    public static final short INDEX_SIZE = INT_SIZE;
    public static final short SIZE_SIZE = INT_SIZE;
    public static final short LONG_SIZE = 8;

    public ByteBuffer serialize(Map<Integer, List<Record>> recordMap) {
        Header header = new Header(recordMap);
        ByteBuffer buffer = ByteBuffer.allocate(header.getSize());

        serialize(recordMap, header.getOffsets(), buffer);
        return buffer;
    }

    public int serialize(Map<Integer, List<Record>> recordMap, String name) {
        Header header = new Header(recordMap);

        try {
            Files.deleteIfExists(Paths.get(name));
            FileChannel channel = new RandomAccessFile(name, "rw").getChannel();
            MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, header.getSize());
            serialize(recordMap, header.getOffsets(), mappedByteBuffer);

            channel.close();

            return header.getSize();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void serialize(Map<Integer, List<Record>> recordMap, Map<Integer, Integer> offsets, ByteBuffer buffer) {
        buffer.putInt(recordMap.size());

        for (Map.Entry<Integer, Integer> entry : offsets.entrySet()) {
            buffer.putInt(entry.getKey());
            buffer.putInt(entry.getValue());
        }

        for (Map.Entry<Integer, List<Record>> entry : recordMap.entrySet()) {
            putRecord(buffer, entry.getValue());
        }

        buffer.flip();
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
