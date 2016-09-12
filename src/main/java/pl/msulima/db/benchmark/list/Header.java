package pl.msulima.db.benchmark.list;

import pl.msulima.db.Record;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Header {

    private final Map<Integer, Integer> offsets;
    private final int size;

    public Header(Map<Integer, List<Record>> recordMap) {
        this.offsets = new HashMap<>(recordMap.size());

        long offset = calculateHeaderSize(recordMap.size());
        for (Map.Entry<Integer, List<Record>> entry : recordMap.entrySet()) {
            offsets.put(entry.getKey(), (int) offset);
            offset += calculateRecordSize(entry);
            if (offset > Integer.MAX_VALUE) {
                throw new IllegalStateException("Offset to large " + offset);
            }
        }
        this.size = (int) offset;
    }

    public Map<Integer, Integer> getOffsets() {
        return Collections.unmodifiableMap(offsets);
    }

    public int getSize() {
        return size;
    }

    public int calculateHeaderSize(int size) {
        return Serializer.SIZE_SIZE + size * (Serializer.KEY_SIZE + Serializer.INDEX_SIZE);
    }

    private int calculateRecordSize(Map.Entry<Integer, List<Record>> entry) {
        return Serializer.SIZE_SIZE + entry.getValue().size() * Record.SIZE;
    }
}
