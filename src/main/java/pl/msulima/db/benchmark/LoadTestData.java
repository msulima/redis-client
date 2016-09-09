package pl.msulima.db.benchmark;

import pl.msulima.db.Record;
import pl.msulima.db.Serializer;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class LoadTestData {

    public static final int LIMIT = 400_000;

    public static void main(String... args) {
        Map<Integer, List<Record>> recordMap = new HashMap<>();

        Random random = ThreadLocalRandom.current();
        for (int i = 0; i < LIMIT; i++) {
            int size = random.nextInt(400);
            if (i % 1000 == 0) {
                size = 50_000;
                System.out.print(".");
            }
            List<Record> records = new ArrayList<>(size);

            for (int j = 0; j < size; j++) {
                records.add(new Record(i, j, size));
            }
            recordMap.put(i, records);
        }

        System.out.println("\nWriting...");
        int written = new Serializer().serialize(recordMap, "test1.bin");
        System.out.printf("%d bytes written%n", written);
    }
}
