package com.jivesoftware.os.lab.guts.allocators;

import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.allocators.LABMemoryAllocator;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 *
 * @author jonathan.colt
 */
public class LABIndexableMemory {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String name;
    private final LABMemoryAllocator memoryAllocator;

    public LABIndexableMemory(String name,
        LABMemoryAllocator memoryAllocator) {

        this.name = name;
        this.memoryAllocator = memoryAllocator;
    }

    void acquireBytes(long chunkAddress, BolBuffer bolBuffer) throws Exception {
        if (chunkAddress == -1) {
            bolBuffer.allocate(-1);
            return;
        }
        memoryAllocator.acquireBytes(chunkAddress, bolBuffer);
    }

    public byte[] bytes(long chunkAddress) throws InterruptedException {
        if (chunkAddress == -1) {
            return null;
        }
        return memoryAllocator.bytes(chunkAddress);
    }

    public long allocate(BolBuffer bolBuffer) throws Exception {
        if (bolBuffer == null || bolBuffer.length == -1) {
            throw new IllegalStateException();
        }
        return memoryAllocator.allocate(bolBuffer.bytes, bolBuffer.offset, bolBuffer.length);
    }

    public long allocate(byte[] bytes) throws Exception {
        if (bytes == null) {
            throw new IllegalStateException();
        }
        return memoryAllocator.allocate(bytes, 0, bytes.length);
    }

    public void release(long address) throws Exception {
        if (address == -1) {
            return;
        }
        memoryAllocator.release(address);
    }

    public int compare(Rawhide rawhide, long left, long right) {
        return memoryAllocator.compare(rawhide, left, right);
    }

    public int compare(Rawhide rawhide, long left, byte[] right, int rightOffset, int rightLength) {
        return memoryAllocator.compare(rawhide, left, right, rightOffset, rightLength);
    }

    public int compare(Rawhide rawhide, byte[] left, int leftOffset, int leftLength, long right) {
        return memoryAllocator.compare(rawhide, left, leftOffset, leftLength, right);
    }

    public int compare(Rawhide rawhide, byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        return memoryAllocator.compare(rawhide, left, leftOffset, leftLength, right, rightOffset, rightLength);
    }

}
