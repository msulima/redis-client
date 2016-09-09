package pl.msulima.db.benchmark;

import pl.msulima.db.Reader;

public class ReadTestData {

    public static void main(String... args) {
        Reader reader = Reader.fromFile("test1.bin");

        for (int i = 0; i < 10; i++) {
            run(reader);
        }
    }

    private static void run(Reader reader) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < LoadTestData.LIMIT; i++) {
            reader.get(i);
        }
        long took = System.currentTimeMillis() - start;
        System.out.printf("Took: %d ms (%.0f ops/sec)%n", took, 1000d * LoadTestData.LIMIT / (took + 1));
    }
}
