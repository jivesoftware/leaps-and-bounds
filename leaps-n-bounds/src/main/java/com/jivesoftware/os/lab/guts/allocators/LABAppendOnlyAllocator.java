package com.jivesoftware.os.lab.guts.allocators;

import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.io.api.UIO;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class LABAppendOnlyAllocator implements LABMemoryAllocator {

    private volatile byte[] memory = null;
    private final int maxAllocationCount = Integer.MAX_VALUE / 2;
    private final Semaphore allocated = new Semaphore(maxAllocationCount);
    private final AtomicLong allocateNext = new AtomicLong();
    private final AtomicLong freed = new AtomicLong();

    private final int maxAllocatableInBytes;
    private final Callable<Void> calledOnOOM;

    public LABAppendOnlyAllocator(Callable<Void> calledOnOOM) {
        this(1024 * 1024 * 100, calledOnOOM);
    }

    public LABAppendOnlyAllocator(int maxAllocatableInBytes, Callable<Void> calledOnOOM) {
        this.maxAllocatableInBytes = maxAllocatableInBytes;
        this.calledOnOOM = calledOnOOM;
    }

    @Override
    public boolean acquireBytes(long address, BolBuffer bolBuffer) {
        byte[] stackCopy = memory;
        bolBuffer.bytes = stackCopy;
        bolBuffer.offset = (int) (address + 4);
        bolBuffer.length = UIO.bytesInt(stackCopy, (int) address);
        return true;
    }

    @Override
    public byte[] bytes(long address) {
        if (address == -1) {
            return null;
        }
        byte[] stackCopy = memory;
        int length = UIO.bytesInt(stackCopy, (int) address);
        if (length > -1) {
            byte[] copy = new byte[length];
            System.arraycopy(stackCopy, (int) address + 4, copy, 0, length);
            return copy;
        } else {
            throw new IllegalStateException("Address:" + address + " length=" + length);
        }

    }

    public long allocate(BolBuffer bytes) throws Exception {
        if (bytes == null || bytes.length == -1) {
            return -1;
        }
        return allocate(bytes.bytes, bytes.offset, bytes.length);
    }

    public long allocate(byte[] bytes) throws Exception {
        return allocate(bytes, 0, bytes.length);
    }

    @Override
    public long allocate(byte[] bytes, int offset, int length) throws Exception {
        if (bytes == null) {
            return -1;
        }
        int address = allocate(length);
        synchronized (this) {
            UIO.intBytes(length, memory, address);
            System.arraycopy(bytes, offset, memory, address + 4, length);
        }

        return address;
    }

    private int allocate(int length) throws Exception {
        long address;
        while (true) {
            if (allocateNext.get() + 4 + length > maxAllocatableInBytes) {
                calledOnOOM.call();
                Thread.yield();
            } else {
                allocated.acquire();
                address = allocateNext.getAndAdd(4 + length);
                if (address + 4 + length > maxAllocatableInBytes) {
                    allocated.release();
                    calledOnOOM.call();
                } else {
                    break;
                }
            }
        }
        if (memory == null || memory.length < address + 4 + length) {
            synchronized (this) {
                if (memory == null || memory.length < address + 4 + length) {
                    int power = UIO.chunkPower(address + 4 + length, 4);
                    byte[] bytes = new byte[1 << power];
                    if (memory != null) {
                        System.arraycopy(memory, 0, bytes, 0, memory.length);
                    }
                    memory = bytes;
                }
            }
        }
        return (int) address;
    }

    @Override
    public void release(long address) throws InterruptedException {
        byte[] stackCopy = memory;
        int length = UIO.bytesInt(stackCopy, (int) address);
        if (length > -1) {
            freed.addAndGet(length);
        } else {
            throw new IllegalStateException("Address:" + address + " length=" + length);
        }
        allocated.release();
    }

    @Override
    public int compare(Rawhide rawhide, long leftAddress, long rightAddress) {
        if (leftAddress == -1 && rightAddress == -1) {
            return rawhide.compareBB(null, -1, -1, null, -1, -1);
        } else if (leftAddress == -1) {
            int rightLength = UIO.bytesInt(memory, (int) rightAddress);
            return rawhide.compareBB(null, -1, -1, memory, (int) rightAddress + 4, rightLength);
        } else if (rightAddress == -1) {
            int leftLength = UIO.bytesInt(memory, (int) leftAddress);
            return rawhide.compareBB(memory, (int) leftAddress + 4, leftLength, null, -1, -1);
        } else {
            int leftLength = UIO.bytesInt(memory, (int) leftAddress);
            int rightLength = UIO.bytesInt(memory, (int) rightAddress);
            return rawhide.compareBB(memory, (int) leftAddress + 4, leftLength, memory, (int) rightAddress + 4, rightLength);
        }
    }

    @Override
    public int compare(Rawhide rawhide, long leftAddress, byte[] rightBytes, int rightOffset, int rightLength) {
        if (leftAddress == -1) {
            return rawhide.compareBB(null, -1, -1, rightBytes, rightOffset, rightBytes == null ? -1 : rightLength);
        } else {
            int leftLength = UIO.bytesInt(memory, (int) leftAddress);
            return rawhide.compareBB(memory, (int) leftAddress + 4, leftLength, rightBytes, rightOffset, rightBytes == null ? -1 : rightLength);
        }
    }

    @Override
    public int compare(Rawhide rawhide, byte[] leftBytes, int leftOffset, int leftLength, long rightAddress) {
        if (rightAddress == -1) {
            return rawhide.compareBB(leftBytes, leftOffset, leftBytes == null ? -1 : leftLength, null, -1, -1);
        } else {
            int l2 = UIO.bytesInt(memory, (int) rightAddress);
            return rawhide.compareBB(leftBytes, leftOffset, leftBytes == null ? -1 : leftLength, memory, (int) rightAddress + 4, l2);
        }
    }

    @Override
    public int compare(Rawhide rawhide, byte[] leftBytes, int leftOffset, int leftLength, byte[] rightBytes, int rightOffset, int rightLength) {
        return rawhide.compareBB(leftBytes, leftOffset, leftBytes == null ? -1 : leftLength, rightBytes, rightOffset, rightBytes == null ? -1 : rightLength);
    }

    public void freeAll() throws InterruptedException {

        allocated.acquire(maxAllocationCount);
        try {
            memory = null;
            allocateNext.set(0);
        } finally {
            allocated.release(maxAllocationCount);
        }

    }
}
