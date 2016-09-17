package com.jivesoftware.os.lab.guts.allocators;

import com.jivesoftware.os.lab.api.rawhide.Rawhide;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 *
 * @author jonathan.colt
 */
public class LABIndexableMemory {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String name;
    private final LABAppendOnlyAllocator memoryAllocator;

    public LABIndexableMemory(String name,
        LABAppendOnlyAllocator memoryAllocator) {

        this.name = name;
        this.memoryAllocator = memoryAllocator;
    }

    void acquireBytes(long address, BolBuffer bolBuffer) throws Exception {
        if (address == -1) {
            bolBuffer.allocate(-1);
            return;
        }
        memoryAllocator.acquireBytes(address, bolBuffer);
    }

    public byte[] bytes(long address) throws InterruptedException {
        if (address == -1) {
            return null;
        }
        return memoryAllocator.bytes(address);
    }

    public long allocate(BolBuffer bolBuffer, LABCostChangeInBytes costInBytes) throws Exception {
        if (bolBuffer == null || bolBuffer.length == -1) {
            throw new IllegalStateException();
        }
        return memoryAllocator.allocate(bolBuffer.bytes, bolBuffer.offset, bolBuffer.length, costInBytes);
    }


    public void release(long address) throws Exception {
        if (address == -1) {
            return;
        }
        memoryAllocator.release(address);
    }


    public int compareLB(Rawhide rawhide, long left, byte[] right, int rightOffset, int rightLength) {
        return memoryAllocator.compareLB(rawhide, left, right, rightOffset, rightLength);
    }

    public int compareBL(Rawhide rawhide, byte[] left, int leftOffset, int leftLength, long right) {
        return memoryAllocator.compareBL(rawhide, left, leftOffset, leftLength, right);
    }

    public int compareBB(Rawhide rawhide, byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        return memoryAllocator.compareBB(rawhide, left, leftOffset, leftLength, right, rightOffset, rightLength);
    }

}
