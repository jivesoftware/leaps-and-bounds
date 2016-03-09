package com.jivesoftware.os.lab.guts;

import java.util.Arrays;

/**
 *
 * @author jonathan.colt
 */
public class MergeRange {

    final long generation;
    final int offset;
    final int length;
    final byte[] minKey;
    final byte[] maxKey;

    public MergeRange(long generation, int startOfSmallestMerge, int length, byte[] minKey, byte[] maxKey) {
        this.generation = generation;
        this.offset = startOfSmallestMerge;
        this.length = length;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    @Override
    public String toString() {
        return "MergeRange{"
            + "generation=" + generation
            + ", offset=" + offset
            + ", length=" + length
            + ", minKey=" + Arrays.toString(minKey)
            + ", maxKey=" + Arrays.toString(maxKey)
            + '}';
    }
}
