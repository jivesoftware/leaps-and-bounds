package com.jivesoftware.os.lab;

/**
 *
 * @author jonathan.colt
 */
public class MergeRange {

    final int startOfSmallestMerge;
    final int length;

    public MergeRange(int startOfSmallestMerge, int length) {
        this.startOfSmallestMerge = startOfSmallestMerge;
        this.length = length;
    }

}
