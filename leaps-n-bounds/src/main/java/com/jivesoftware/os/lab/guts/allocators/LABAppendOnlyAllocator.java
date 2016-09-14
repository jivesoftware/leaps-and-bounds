package com.jivesoftware.os.lab.guts.allocators;

import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.io.api.UIO;

/**
 *
 * @author jonathan.colt
 */
public class LABAppendOnlyAllocator implements LABMemoryAllocator {

    private volatile byte[][] memory = null;
    private final int powerSize;
    private final long powerMask;
    //private final AtomicLong allocateNext = new AtomicLong();
    private volatile long allocateNext = 0;

    public LABAppendOnlyAllocator(int powerSize) {
        this.powerSize = powerSize;
        this.powerMask = (1 << powerSize) - 1;
    }

    @Override
    public long sizeInBytes() {
        byte[][] stackCopy = memory;
        if (stackCopy == null) {
            return 0;
        }
        long size = 0;
        for (byte[] bs : stackCopy) {
            size += (bs != null) ? bs.length : 0;
        }
        return size;
    }

    @Override
    public boolean acquireBytes(long address, BolBuffer bolBuffer) {
        int index = (int) (address >>> powerSize);
        byte[] stackCopy = memory[index];
        address &= powerMask;

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
        int index = (int) (address >>> powerSize);
        byte[] stackCopy = memory[index];
        address &= powerMask;

        int length = UIO.bytesInt(stackCopy, (int) address);
        if (length > -1) {
            byte[] copy = new byte[length];
            System.arraycopy(stackCopy, (int) address + 4, copy, 0, length);
            return copy;
        } else {
            throw new IllegalStateException("Address:" + address + " length=" + length);
        }

    }

    public long allocate(BolBuffer bytes, LABCostChangeInBytes costInBytes) throws Exception {
        if (bytes == null || bytes.length == -1) {
            return -1;
        }
        return allocate(bytes.bytes, bytes.offset, bytes.length, costInBytes);
    }

    public long allocate(byte[] bytes, LABCostChangeInBytes costInBytes) throws Exception {
        return allocate(bytes, 0, bytes.length, costInBytes);
    }

    @Override
    public long allocate(byte[] bytes, int offset, int length, LABCostChangeInBytes costInBytes) throws Exception {
        if (bytes == null) {
            return -1;
        }
        long address = allocate(length, costInBytes);
        int index = (int) (address >>> powerSize);
        int indexAddress = (int) (address & powerMask);

        synchronized (this) {
            byte[] stackCopy = memory[index];
            UIO.intBytes(length, stackCopy, indexAddress);
            System.arraycopy(bytes, offset, stackCopy, indexAddress + 4, length);
        }

        //System.out.println("allocate:" + address + " " + Arrays.toString(bytes) + " offset:" + offset + " length:" + length);
        return address;
    }

    private long allocate(int length, LABCostChangeInBytes costInBytes) throws Exception {

        synchronized (this) {

            long address = allocateNext;
            int index = (int) (address >>> powerSize);
            int tailIndex = (int) ((address + 4 + length) >>> powerSize);
            if (index == tailIndex) {
                allocateNext = address + 4 + length;
            } else {
                long desiredAddress = tailIndex * (1 << powerSize);
                allocateNext = desiredAddress + 4 + length;
                address = desiredAddress;
                index = (int) (desiredAddress >>> powerSize);
            }

            //System.out.println("address:" + address + " index:" + index + "              " + allocateNext.get());
            int indexAlignedAddress = (int) (address & powerMask);
            if (memory == null || index >= memory.length || memory[index] == null || memory[index].length < indexAlignedAddress + 4 + length) {

                if (memory == null) {
                    //System.out.println("Initial " + this);
                    memory = new byte[index + 1][];
                } else if (index >= memory.length) {
                    //System.out.println("Copy " + this);
                    byte[][] newMemory = new byte[index + 1][];
                    System.arraycopy(memory, 0, newMemory, 0, memory.length);
                    memory = newMemory;
                }

                int power = UIO.chunkPower(indexAlignedAddress + 4 + length, 4);
                byte[] bytes = new byte[1 << power];
                //System.out.println("Grow " + bytes.length + "  " + this);
                if (memory[index] != null) {
                    System.arraycopy(memory[index], 0, bytes, 0, memory[index].length);
                    costInBytes.cost(bytes.length - memory[index].length);
                } else {
                    costInBytes.cost(bytes.length);
                }
                memory[index] = bytes;
            }

            return address;
        }
    }

    @Override
    public void release(long address) throws InterruptedException {
    }

    @Override
    public int compare(Rawhide rawhide, long leftAddress, long rightAddress
    ) {

        if (leftAddress == -1 && rightAddress == -1) {
            return rawhide.compareBB(null, -1, -1, null, -1, -1);
        } else if (leftAddress == -1) {
            int rightIndex = (int) (rightAddress >>> powerSize);
            rightAddress &= powerMask;
            byte[] rightCopy = memory[rightIndex];

            int rightLength = UIO.bytesInt(rightCopy, (int) rightAddress);
            return rawhide.compareBB(null, -1, -1, rightCopy, (int) rightAddress + 4, rightLength);
        } else if (rightAddress == -1) {
            int leftIndex = (int) (leftAddress >>> powerSize);
            leftAddress &= powerMask;
            byte[] leftCopy = memory[leftIndex];

            int leftLength = UIO.bytesInt(leftCopy, (int) leftAddress);
            return rawhide.compareBB(leftCopy, (int) leftAddress + 4, leftLength, null, -1, -1);
        } else {
            int leftIndex = (int) (leftAddress >>> powerSize);
            leftAddress &= powerMask;
            byte[] leftCopy = memory[leftIndex];

            int rightIndex = (int) (rightAddress >>> powerSize);
            rightAddress &= powerMask;
            byte[] rightCopy = memory[rightIndex];

            int leftLength = UIO.bytesInt(leftCopy, (int) leftAddress);
            int rightLength = UIO.bytesInt(rightCopy, (int) rightAddress);
            return rawhide.compareBB(leftCopy, (int) leftAddress + 4, leftLength, rightCopy, (int) rightAddress + 4, rightLength);
        }
    }

    @Override
    public int compare(Rawhide rawhide, long leftAddress, byte[] rightBytes, int rightOffset, int rightLength
    ) {
        if (leftAddress == -1) {
            return rawhide.compareBB(null, -1, -1, rightBytes, rightOffset, rightBytes == null ? -1 : rightLength);
        } else {
            int leftIndex = (int) (leftAddress >>> powerSize);
            leftAddress &= powerMask;
            byte[] leftCopy = memory[leftIndex];

            int leftLength = UIO.bytesInt(leftCopy, (int) leftAddress);
            return rawhide.compareBB(leftCopy, (int) leftAddress + 4, leftLength, rightBytes, rightOffset, rightBytes == null ? -1 : rightLength);
        }
    }

    @Override
    public int compare(Rawhide rawhide, byte[] leftBytes, int leftOffset, int leftLength, long rightAddress
    ) {
        if (rightAddress == -1) {
            return rawhide.compareBB(leftBytes, leftOffset, leftBytes == null ? -1 : leftLength, null, -1, -1);
        } else {

            int rightIndex = (int) (rightAddress >>> powerSize);
            rightAddress &= powerMask;
            byte[] rightCopy = memory[rightIndex];

            int l2 = UIO.bytesInt(rightCopy, (int) rightAddress);
            return rawhide.compareBB(leftBytes, leftOffset, leftBytes == null ? -1 : leftLength, rightCopy, (int) rightAddress + 4, l2);
        }
    }

    @Override
    public int compare(Rawhide rawhide, byte[] leftBytes, int leftOffset, int leftLength, byte[] rightBytes, int rightOffset, int rightLength
    ) {
        return rawhide.compareBB(leftBytes, leftOffset, leftBytes == null ? -1 : leftLength, rightBytes, rightOffset, rightBytes == null ? -1 : rightLength);
    }

    public void freeAll() throws InterruptedException {
        synchronized (this) {
            //System.out.println("-----freeAll-----");
            memory = null;
            allocateNext = 0;
        }
    }
}
