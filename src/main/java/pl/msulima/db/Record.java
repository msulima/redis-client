package pl.msulima.db;

import com.google.common.base.MoreObjects;

import java.util.Objects;

class Record {

    public static final short SIZE = Serializer.INT_SIZE * 2 + Serializer.LONG_SIZE;

    public final int a;
    public final int b;
    public final long c;

    Record(int a, int b, long c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return a == record.a &&
                b == record.b &&
                c == record.c;
    }

    @Override
    public int hashCode() {
        return Objects.hash(a, b, c);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("a", a)
                .add("b", b)
                .add("c", c)
                .toString();
    }
}
