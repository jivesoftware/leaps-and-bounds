package org.apache.commons.collections4.trie;

import com.google.common.primitives.UnsignedBytes;
import java.util.Arrays;

public class LexKey implements Comparable<LexKey> {

    final byte[] bytes;

    public LexKey(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LexKey key = (LexKey) o;

        return Arrays.equals(bytes, key.bytes);

    }

    @Override
    public int hashCode() {
        return bytes != null ? Arrays.hashCode(bytes) : 0;
    }

    @Override
    public String toString() {
        return "LexKey{" + "bytes=" + Arrays.toString(bytes) + '}';
    }

    @Override
    public int compareTo(LexKey o) {
        return UnsignedBytes.lexicographicalComparator().compare(bytes, o.bytes);
    }

}
