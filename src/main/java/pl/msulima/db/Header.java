package pl.msulima.db;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static pl.msulima.db.Serializer.*;

public class Header {

    private final Map<Integer, Integer> offsets;
    private final int size;

    public Header(Map<Integer, List<Record>> recordMap) {
        this.offsets = new HashMap<>(recordMap.size());

        int offset = calculateHeaderSize(recordMap.size());
        for (Map.Entry<Integer, List<Record>> entry : recordMap.entrySet()) {
            offsets.put(entry.getKey(), offset);
            offset += calculateRecordSize(entry);
        }
        this.size = offset;
    }

    public Map<Integer, Integer> getOffsets() {
        return Collections.unmodifiableMap(offsets);
    }

    public int getSize() {
        return size;
    }

    public int calculateHeaderSize(int size) {
        return SIZE_SIZE + size * (KEY_SIZE + INDEX_SIZE);
    }

    private int calculateRecordSize(Map.Entry<Integer, List<Record>> entry) {
        return SIZE_SIZE + entry.getValue().size() * Record.SIZE;
    }
}
