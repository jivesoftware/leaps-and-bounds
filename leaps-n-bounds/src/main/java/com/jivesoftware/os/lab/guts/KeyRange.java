package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.Rawhide;
import java.util.Arrays;

/**
 *
 * @author jonathan.colt
 */
public class KeyRange implements Comparable<KeyRange> {

    final Rawhide rawhide;
    final byte[] start;
    final byte[] end;

    public KeyRange(Rawhide rawhide, byte[] start, byte[] end) {
        this.rawhide = rawhide;
        this.start = start;
        this.end = end;
    }

    boolean contains(KeyRange range) {
        if (start == null || end == null) {
            return false;
        }
        return rawhide.compare(start, range.start) <= 0 && rawhide.compare(end, range.end) >= 0;
    }

    @Override
    public int compareTo(KeyRange o) {
        
        int c = rawhide.compare(start, o.start);
        if (c == 0) {
            c = rawhide.compare(o.end, end); // reversed
        }
        return c;
    }

    @Override
    public String toString() {
        return "KeyRange{" + "start=" + Arrays.toString(start) + ", end=" + Arrays.toString(end) + '}';
    }

    public KeyRange join(KeyRange id) {
        return new KeyRange(rawhide, min(start, id.start), max(end, id.end));
    }

    private byte[] min(byte[] a, byte[] b) {
        return rawhide.compare(a, b) < 0 ? a : b;
    }

    private byte[] max(byte[] a, byte[] b) {
        return rawhide.compare(a, b) > 0 ? a : b;
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
