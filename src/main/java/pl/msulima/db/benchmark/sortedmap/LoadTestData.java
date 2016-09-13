package pl.msulima.db.benchmark.sortedmap;

import pl.msulima.db.Record;

import java.util.ArrayList;
import java.util.List;

public class LoadTestData {

    public static final int LIMIT = 800;
    public static final int MAX_UNITS = 50_000;

    public static void main(String... args) {
        List<Pair> recordMap = new ArrayList<>(LIMIT * MAX_UNITS);

//        Random random = ThreadLocalRandom.current();
        for (int i = 0; i < LIMIT; i++) {
//            int size = random.nextInt(400);
            int size = MAX_UNITS;
            if (i % 100 == 0) {
                System.out.print(".");
            }

            for (int j = 0; j < size; j++) {
                recordMap.add(new Pair(i * size + j, new Record(i, j, size, 1, 2, 3)));
            }
        }

        System.out.println("\nWriting...");
        int written = new Serializer().serialize(recordMap, "test1.bin");
        System.out.printf("%d MB written%n", written / 1024 / 1024);
    }
}
