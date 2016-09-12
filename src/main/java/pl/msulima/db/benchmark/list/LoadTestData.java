package pl.msulima.db.benchmark.list;

import pl.msulima.db.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadTestData {

    public static final int LIMIT = 1_000;
    public static final int MAX_UNITS = 50_000;

    public static void main(String... args) {
        Map<Integer, List<Record>> recordMap = new HashMap<>();

//        Random random = ThreadLocalRandom.current();
        for (int i = 0; i < LIMIT; i++) {
//            int size = random.nextInt(400);
            int size = MAX_UNITS;
            if (i % 1000 == 0) {
                System.out.print(".");
            }
            List<Record> records = new ArrayList<>(size);

            for (int j = 0; j < size; j++) {
                records.add(new Record(i, j, size, 1, 2, 3));
            }
            recordMap.put(i, records);
        }

        System.out.println("\nWriting...");
        int written = new Serializer().serialize(recordMap, "test1.bin");
        System.out.printf("%d bytes written%n", written);
    }
}
