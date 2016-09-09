package pl.msulima.db.benchmark;

import org.apache.commons.math3.distribution.GammaDistribution;
import pl.msulima.db.Record;
import pl.msulima.db.Serializer;

import java.util.*;

public class LoadTestData {

    public static final int LIMIT = 200_000;

    public static void main(String... args) {
        Map<Integer, List<Record>> recordMap = new LinkedHashMap<>();

        GammaDistribution distribution = new GammaDistribution(10, 30);
        List<Integer> ids = new ArrayList<>(LIMIT);
        for (int i = 0; i < LIMIT; i++) {
            ids.add(i);
        }
        Collections.shuffle(ids);

        for (int i = 0; i < LIMIT; i++) {
            int size = (int) Math.round(distribution.sample());
            if (i % 1000 == 0) {
                size = 50_000;
            }
            List<Record> records = new ArrayList<>(size);

            for (int j = 0; j < size; j++) {
                records.add(new Record(i, j, size));
            }
            recordMap.put(ids.get(i), records);
        }

        System.out.println("Writing...");
        int written = new Serializer().serialize(recordMap, "test1.bin");
        System.out.printf("%d bytes written%n", written);
    }
}
