package pl.msulima.db.benchmark;

import pl.msulima.db.Reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ReadTestData {

    public static void main(String... args) {
        Reader reader = Reader.fromFile("test1.bin");

        List<Integer> ids = new ArrayList<>(LoadTestData.LIMIT);
        for (int i = 0; i < LoadTestData.LIMIT; i++) {
            ids.add(i);
        }
        Collections.shuffle(ids);

        for (int i = 0; i < 10; i++) {
            run(reader, ids);
        }
    }

    private static void run(Reader reader, List<Integer> ids) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < LoadTestData.LIMIT; i++) {
            reader.get(ids.get(i));
        }
        long took = System.currentTimeMillis() - start;
        double opsSec = 1000d * LoadTestData.LIMIT / (took + 1);
        double usOp = 1000d * took / LoadTestData.LIMIT;
        System.out.printf("Took: %d ms (%.0f ops/sec %.1f us/op)%n", took, opsSec, usOp);
    }
}
