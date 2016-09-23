package com.jivesoftware.os.lab.guts.allocators;

import com.jivesoftware.os.lab.api.rawhide.Rawhide;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 *
 * @author jonathan.colt
 */
public class LABAppendOnlyAllocator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static class Memory {

        final byte[][] slabs;
        final int powerSize;
        final int powerLength;
        final long powerMask;

        public Memory(int powerSize, byte[][] slabs) {
            this.powerSize = powerSize;
            this.powerLength = 1 << powerSize;
            this.powerMask = (powerLength) - 1;
            this.slabs = slabs;
        }
    }

    private static final int MIN_POWER = 3;
    private static final int MAX_POWER = 31;

    private final String name;
    private volatile Memory memory;
    private volatile long allocateNext = 0;

//    private final Object[] freePointerLocks = new Object[32];
//    private final int[] freePointer = new int[32];

    public LABAppendOnlyAllocator(String name, int initialPower) {
        this.name = name;
        int power = Math.min(Math.max(MIN_POWER, initialPower), MAX_POWER);
        this.memory = new Memory(power, null);
//        for (int i = 0; i < freePointerLocks.length; i++) {
//            freePointerLocks[i] = new Object();
//            freePointer[i] = -1;
//        }
    }

    public int poweredUpTo() {
        return memory.powerSize;
    }

    public boolean acquireBytes(long address, BolBuffer bolBuffer) {
        Memory m = memory;
        int index = (int) (address >>> m.powerSize);
        byte[] stackCopy = m.slabs[index];
        address &= m.powerMask;

        bolBuffer.force(stackCopy, (int) (address + 4), UIO.bytesInt(stackCopy, (int) address));
        return true;
    }

    public byte[] bytes(long address) {
        if (address == -1) {
            return null;
        }
        Memory m = memory;
        int index = (int) (address >>> m.powerSize);
        byte[] stackCopy = m.slabs[index];
        address &= m.powerMask;

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
        long address = allocate(4 + length, costInBytes);
        Memory m = memory;
        int index = (int) (address >>> m.powerSize);
        int indexAddress = (int) (address & m.powerMask);

        synchronized (this) {
            byte[] stackCopy = m.slabs[index];
            UIO.intBytes(length, stackCopy, indexAddress);
            System.arraycopy(bytes, offset, stackCopy, indexAddress + 4, length);
        }
        return address;
    }

    private long allocate(int length, LABCostChangeInBytes costInBytes) throws Exception {

        int power = UIO.chunkPower(length, MIN_POWER);
//        if (freePointer[power] != -1) {
//            synchronized (freePointerLocks[power]) {
//                if (freePointer[power] != -1) {
//                    long address = freePointer[power];
//                    Memory m = memory;
//                    int index = (int) (address >>> m.powerSize);
//                    int indexAddress = (int) (address & m.powerMask);
//                    freePointer[power] = UIO.bytesInt(m.slabs[index], indexAddress);
//                    costInBytes.cost(0, 1 << power);
//                    return address;
//                }
//            }
//        }
        synchronized (this) {

            Memory m = memory;
            while (length > m.powerLength && m.powerSize < MAX_POWER) {
                LOG.warn("Uping Power to {}  because of length={} for {}. Consider changing you config to {}.", m.powerSize + 1, length, name, m.powerSize + 1);
                m = powerUp(m);
            }
            memory = m;

            long address = allocateNext;
            int index = (int) (address >>> m.powerSize);
            int tailIndex = (int) ((address + length) >>> m.powerSize);
            if (index == tailIndex) {
                allocateNext = address + length;
            } else {
                long desiredAddress = tailIndex * (1 << m.powerSize);
                allocateNext = desiredAddress + length;
                address = desiredAddress;
                index = (int) (desiredAddress >>> m.powerSize);
            }

            int indexAlignedAddress = (int) (address & m.powerMask);
            if (m.slabs == null
                || index >= m.slabs.length
                || m.slabs[index] == null
                || m.slabs[index].length < indexAlignedAddress + length) {

                if (m.slabs == null) {
                    if (index != 0) {
                        System.out.println("Ouch WTF exepcted 0 but was " + index + " address=" + address + " length=" + length + " power=" + m.powerSize);
                        System.exit(1);
                    }
                    m = new Memory(m.powerSize, new byte[index + 1][]);
                } else if (index >= m.slabs.length) {

                    byte[][] newMemory = new byte[index + 1][];
                    System.arraycopy(m.slabs, 0, newMemory, 0, m.slabs.length);
                    m = new Memory(m.powerSize, newMemory);
                }

                power = UIO.chunkPower(indexAlignedAddress + length, MIN_POWER);
                byte[] bytes = new byte[1 << power];
                if (m.slabs[index] != null) {
                    System.arraycopy(m.slabs[index], 0, bytes, 0, m.slabs[index].length);
                    costInBytes.cost(bytes.length - m.slabs[index].length, 0);
                } else {
                    costInBytes.cost(bytes.length, 0);
                }
                m.slabs[index] = bytes; // uck

                if (m.slabs.length >= 2 && m.powerSize < MAX_POWER) { // config?
                    LOG.warn("Uping Power to {} because slab count={} for {}. Consider changing you config to {}.",
                        m.powerSize + 1, m.slabs.length, name, m.powerSize + 1);
                    m = powerUp(m);
                }
                memory = m;
            }

            return address;
        }
    }

    static Memory powerUp(Memory m) {
        Memory nm;
        int nextPowerSize = m.powerSize + 1;
        if (nextPowerSize > MAX_POWER) {
            return m;
        }
        if (m.slabs == null) {
            nm = new Memory(nextPowerSize, null);
        } else if (m.slabs.length == 1) {
            nm = new Memory(nextPowerSize, m.slabs);
        } else {
            int slabLength = m.slabs.length;
            int numSlabs = (m.slabs.length / 2) + (slabLength % 2 == 0 ? 0 : 1);
            byte[][] newSlabs = new byte[numSlabs][];
            int offset = m.powerLength;
            for (int i = 0, npi = 0; i < slabLength; i += 2, npi++) {
                if (i == slabLength - 1) {
                    newSlabs[npi] = m.slabs[i];
                } else {
                    newSlabs[npi] = new byte[1 << nextPowerSize];
                    System.arraycopy(m.slabs[i], 0, newSlabs[npi], 0, m.slabs[i].length);
                    System.arraycopy(m.slabs[i + 1], 0, newSlabs[npi], offset, m.slabs[i + 1].length);
                }
            }
            nm = new Memory(nextPowerSize, newSlabs);
        }
        return nm;
    }

    public int release(long address) throws InterruptedException {
        if (address == -1) {
            return 0;
        }
        Memory m = memory;
        int index = (int) (address >>> m.powerSize);
        int indexAddress = (int) (address & m.powerMask);
        int length = UIO.bytesInt(m.slabs[index], indexAddress);
        int power = UIO.chunkPower(4 + length, MIN_POWER);
//        synchronized (freePointerLocks[power]) {
//            UIO.intBytes(freePointer[power], m.slabs[index], indexAddress);
//            freePointer[power] = (int) address;
//        }
        return 1 << power;
    }

    public int compareLB(Rawhide rawhide, long leftAddress, byte[] rightBytes, int rightOffset, int rightLength
    ) {
        if (leftAddress == -1) {
            return rawhide.compareBB(null, -1, -1, rightBytes, rightOffset, rightBytes == null ? -1 : rightLength);
        } else {
            Memory m = memory;
            int leftIndex = (int) (leftAddress >>> m.powerSize);
            leftAddress &= m.powerMask;
            byte[] leftCopy = m.slabs[leftIndex];

            int leftLength = UIO.bytesInt(leftCopy, (int) leftAddress);
            return rawhide.compareBB(leftCopy, (int) leftAddress + 4, leftLength, rightBytes, rightOffset, rightBytes == null ? -1 : rightLength);
        }
    }

    public int compareBL(Rawhide rawhide, byte[] leftBytes, int leftOffset, int leftLength, long rightAddress
    ) {
        if (rightAddress == -1) {
            return rawhide.compareBB(leftBytes, leftOffset, leftBytes == null ? -1 : leftLength, null, -1, -1);
        } else {
            Memory m = memory;
            int rightIndex = (int) (rightAddress >>> m.powerSize);
            rightAddress &= m.powerMask;
            byte[] rightCopy = m.slabs[rightIndex];

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
            memory = null;
            allocateNext = 0;
        }
    }
}
