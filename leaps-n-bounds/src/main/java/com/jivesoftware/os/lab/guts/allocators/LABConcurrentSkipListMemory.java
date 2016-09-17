package com.jivesoftware.os.lab.guts.allocators;

import com.jivesoftware.os.lab.api.rawhide.Rawhide;
import com.jivesoftware.os.lab.io.BolBuffer;
import java.util.Comparator;

/**
 *
 * @author jonathan.colt
 */
public class LABConcurrentSkipListMemory {

    private final Rawhide rawhide;
    private final Comparator<byte[]> comparator;
    private final LABIndexableMemory indexableMemory;

    public LABConcurrentSkipListMemory(Rawhide rawhide, LABIndexableMemory indexableMemory) {

        this.rawhide = rawhide;
        this.indexableMemory = indexableMemory;
        this.comparator = rawhide.getKeyComparator();
    }

    public byte[] bytes(long chunkAddress) throws InterruptedException {
        return indexableMemory.bytes(chunkAddress);
    }

    public void acquireBytes(long chunkAddress, BolBuffer bolBuffer) throws Exception {
        indexableMemory.acquireBytes(chunkAddress, bolBuffer);
    }

    public long allocate(BolBuffer bytes, LABCostChangeInBytes costInBytes) throws Exception {
        return indexableMemory.allocate(bytes, costInBytes);
    }

    public void release(long address) throws Exception {
        indexableMemory.release(address);
    }

    public int compareLB(long left, byte[] right, int rightOffset, int rightLength) {
        return indexableMemory.compareLB(rawhide, left, right, rightOffset, rightLength);
    }

    public int compareBL(byte[] left, int leftOffset, int leftLength, long right) {
        return indexableMemory.compareBL(rawhide, left, leftOffset, leftLength, right);
    }

    public int compareBB(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        return indexableMemory.compareBB(rawhide, left, leftOffset, leftLength, right, rightOffset, rightLength);
    }

    Comparator<? super byte[]> bytesComparator() {
        return comparator;
    }

    public void freeAll() {
    }
}
