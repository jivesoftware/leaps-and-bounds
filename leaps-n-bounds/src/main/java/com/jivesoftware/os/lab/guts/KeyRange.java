package com.jivesoftware.os.lab.guts;

import com.google.common.primitives.UnsignedBytes;
import java.util.Arrays;
import java.util.Comparator;

/**
 *
 * @author jonathan.colt
 */
public class KeyRange implements Comparable<KeyRange> {

    final byte[] start;
    final byte[] end;

    public KeyRange(byte[] start, byte[] end) {
        this.start = start;
        this.end = end;
    }

    boolean contains(KeyRange range) {
        if (start == null || end == null) {
            return false;
        }
        Comparator<byte[]> c = UnsignedBytes.lexicographicalComparator();
        return c.compare(start, range.start) <= 0 && c.compare(end, range.end) >= 0;
    }

    @Override
    public int compareTo(KeyRange o) {
        
        int c = UnsignedBytes.lexicographicalComparator().compare(start, o.start);
        if (c == 0) {
            c = UnsignedBytes.lexicographicalComparator().compare(o.end, end); // reversed
        }
        return c;
    }

    @Override
    public String toString() {
        return "KeyRange{" + "start=" + Arrays.toString(start) + ", end=" + Arrays.toString(end) + '}';
    }

    public KeyRange join(KeyRange id) {
        return new KeyRange(min(start, id.start), max(end, id.end));
    }

    private byte[] min(byte[] a, byte[] b) {
        Comparator<byte[]> c = UnsignedBytes.lexicographicalComparator();
        return c.compare(a, b) < 0 ? a : b;
    }

    private byte[] max(byte[] a, byte[] b) {
        Comparator<byte[]> c = UnsignedBytes.lexicographicalComparator();
        return c.compare(a, b) > 0 ? a : b;
    }

    @Override
    public int hashCode() {
        throw new IllegalAccessError("nope");
    }

    @Override
    public boolean equals(Object obj) {
        throw new IllegalAccessError("nope");
    }

}
