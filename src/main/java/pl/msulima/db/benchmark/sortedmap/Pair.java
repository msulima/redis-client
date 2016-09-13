package pl.msulima.db.benchmark.sortedmap;

import pl.msulima.db.Record;

public class Pair {

    public int getKey() {
        return key;
    }

    public Record getRecord() {
        return record;
    }

    private final int key;
    private final Record record;

    public Pair(int key, Record record) {
        this.key = key;
        this.record = record;
    }
}
