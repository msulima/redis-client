package pl.msulima.db;

import com.google.common.base.MoreObjects;
import pl.msulima.db.benchmark.list.Serializer;

import java.util.Objects;

public class Record {

    public static final int SIZE = Serializer.INT_SIZE * 2 + 4 * Serializer.LONG_SIZE;

    public final int a;
    public final int b;
    public final long c;
    public final long d;
    public final long e;
    public final long f;

    public Record(int a, int b, long c, long d, long e, long f) {
        this.a = a;
        this.b = b;
        this.c = c;
        this.d = d;
        this.e = e;
        this.f = f;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return a == record.a &&
                b == record.b &&
                c == record.c &&
                d == record.d &&
                e == record.e &&
                f == record.f;
    }

    @Override
    public int hashCode() {
        return Objects.hash(a, b, c, d, e, f);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("a", a)
                .add("b", b)
                .add("c", c)
                .add("d", d)
                .add("e", e)
                .add("f", f)
                .toString();
    }
}
