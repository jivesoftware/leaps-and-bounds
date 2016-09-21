package com.jivesoftware.os.lab.guts.allocators;

import com.jivesoftware.os.lab.api.rawhide.Rawhide;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;

/**
 *
 * @author jonathan.colt
 */
public class LABAppendOnlyAllocator {

    private volatile byte[][] memory = null;
    private final int powerSize;
    private final long powerMask;
    private volatile long allocateNext = 0;

    public LABAppendOnlyAllocator(int powerSize) {
        this.powerSize = powerSize;
        this.powerMask = (1 << powerSize) - 1;
    }

    public boolean acquireBytes(long address, BolBuffer bolBuffer) {
        int index = (int) (address >>> powerSize);
        byte[] stackCopy = memory[index];
        address &= powerMask;

        bolBuffer.force(stackCopy, (int) (address + 4), UIO.bytesInt(stackCopy, (int) address));
        return true;
    }

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

    public int release(long address) throws InterruptedException {
        if (address == -1) {
            return 0;
        }
        int index = (int) (address >>> powerSize);
        byte[] stackCopy = memory[index];
        address &= powerMask;
        return UIO.bytesInt(stackCopy, (int) address);
    }

    public int compareLB(Rawhide rawhide, long leftAddress, byte[] rightBytes, int rightOffset, int rightLength
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

    public int compareBL(Rawhide rawhide, byte[] leftBytes, int leftOffset, int leftLength, long rightAddress
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

    public int compareBB(Rawhide rawhide, byte[] leftBytes, int leftOffset, int leftLength, byte[] rightBytes, int rightOffset, int rightLength
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
