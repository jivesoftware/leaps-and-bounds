package com.jivesoftware.os.lab.guts.allocators;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.concurrent.Callable;

/**
 *
 * @author jonathan.colt
 */
public class LABHardLocksBuddyAllocator implements LABMemoryAllocator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String name;
    private final byte minPowerSize;
    private final byte maxPowerSize;
    final byte[] memory;
    private final Callable<Void> calledOnFull;
    private final int[] freePowerAddress;

    public LABHardLocksBuddyAllocator(String name, byte minPowerSize, byte maxPowerSize, Callable<Void> calledOnFull) {
        Preconditions.checkArgument(minPowerSize > 2);
        Preconditions.checkArgument(maxPowerSize < 32);
        this.name = name;
        this.minPowerSize = minPowerSize;
        this.maxPowerSize = maxPowerSize;
        this.memory = new byte[(int) UIO.chunkLength(maxPowerSize)];
        this.calledOnFull = calledOnFull;
        this.freePowerAddress = new int[maxPowerSize + 1];
        Arrays.fill(freePowerAddress, -1);

        this.memory[0] = maxPowerSize;
        this.freePowerAddress[maxPowerSize] = 0;
        setNext(0, -1);
        setPrior(0, -1);
    }

    @Override
    public long sizeInBytes() {
        throw new UnsupportedOperationException("TODO.");
    }

    @Override
    public boolean acquireBytes(long address, BolBuffer bolBuffer) {
        bolBuffer.bytes = memory;
        bolBuffer.offset = (int) (address + 1 + 4);
        bolBuffer.length = UIO.bytesInt(memory, (int) address + 1);
        return true;
    }

    @Override
    public byte[] bytes(long address) {
        int length = UIO.bytesInt(memory, (int) address + 1);
        byte[] copy = new byte[length];
        System.arraycopy(memory, (int) address + 1 + 4, copy, 0, length);
        return copy;
    }

    @Override
    public long allocate(byte[] bytes, int offset, int length) {
        if (bytes == null) {
            throw new IllegalStateException();
        }
        int desiredPower = UIO.chunkPower(1 + 4 + length, minPowerSize);

        AGAIN:
        while (true) {

            int allocate = allocate(desiredPower);
            if (allocate != -1) {
                UIO.intBytes(length, memory, allocate + 1);
                System.arraycopy(bytes, offset, memory, allocate + 1 + 4, length);
                return allocate;
            }

            for (int power = desiredPower + 1; power <= maxPowerSize; power++) {
                int splitPower = power - 1;
                int splitPowerLength = (int) UIO.chunkLength(splitPower);

                int address = freePowerAddress[power];
                if (address > -1) {
                    remove(power, address);

                    int buddyAddress = address + splitPowerLength;
                    memory[buddyAddress] = (byte) splitPower;
                    free(splitPower, buddyAddress);

                    memory[address] = (byte) splitPower;
                    free(splitPower, address);
                    continue AGAIN;
                }

            }
            try {
                calledOnFull.call();
            } catch (Exception x) {
                LOG.warn("notifyAllOnFull barfed", x);
            }
            Thread.yield();

        }
    }

    private int allocate(int power) {

        if (freePowerAddress[power] == -1) {
            return -1;
        } else {
            int address = freePowerAddress[power];
            int next = getNext(address);
            clearPrior(next);
            freePowerAddress[power] = next;
            memory[address] = (byte) -memory[address];
            return address;
        }

    }

    @Override
    public void release(long freeAddress) {
        int address;
        byte power;
        byte absPower;

        address = (int) freeAddress;
        power = memory[address];
        absPower = (byte) Math.abs(power);
        memory[address] = absPower;
        free(absPower, address);

        while (true) {

            long chunkLength = UIO.chunkLength(absPower);
            if (((address / chunkLength) & 1) == 0) { // % 2
                int nextAddress = (int) (address + chunkLength);
                if (nextAddress < memory.length && memory[nextAddress] == absPower) {
                    remove(absPower, address);
                    remove(absPower, nextAddress);

                    absPower = (byte) (absPower + 1);
                    memory[address] = absPower;
                    free(absPower, address);
                } else {
                    break;
                }
            } else if ((address - chunkLength) > -1 && memory[(int) (address - chunkLength)] == absPower) {

                remove(absPower, address);
                address = (int) (address - chunkLength);
                remove(absPower, address);

                absPower = (byte) (absPower + 1);
                memory[address] = absPower;
                free(absPower, address);
            } else {
                break;
            }
        }
    }

    private void remove(int power, int address) {
        int prior = getPrior(address);
        int next = getNext(address);

        if (address == freePowerAddress[power]) {
            clearPrior(address);
            clearNext(address);
            clearPrior(next);
            freePowerAddress[power] = next;
        } else {
            if (prior != -1) {
                setNext(prior, next);
            }
            if (next != -1) {
                setPrior(next, prior);
            }
        }
    }

    private void free(int power, int address) {
        if (freePowerAddress[power] == -1) {
            clearPrior(address);
            clearNext(address);
            freePowerAddress[power] = address;
        } else {
            clearPrior(address);
            setNext(address, freePowerAddress[power]);
            setPrior(freePowerAddress[power], address);
            freePowerAddress[power] = address;
        }
    }

    private int getPrior(int address) {
        if (address == -1) {
            return -1;
        }
        return UIO.bytesInt(memory, address + 1);
    }

    private void setPrior(int address, int prior) {
        if (address == -1) {
            throw new IllegalArgumentException("WTF");
        }
        UIO.intBytes(prior, memory, address + 1);
    }

    private void clearPrior(int address) {
        if (address == -1) {
            return;
        }
        UIO.intBytes(-1, memory, address + 1);
    }

    private int getNext(int address) {
        if (address == -1) {
            return -1;
        }
        return UIO.bytesInt(memory, address + 1 + 4);
    }

    private void setNext(int address, int next) {
        if (address == -1) {
            throw new IllegalArgumentException("WTF");
        }
        UIO.intBytes(next, memory, address + 1 + 4);
    }

    private void clearNext(int address) {
        if (address == -1) {
            return;
        }
        UIO.intBytes(-1, memory, address + 1 + 4);
    }

    public void dump() {
        int address = 0;
        System.out.println("Frees:" + Arrays.toString(freePowerAddress));
        while (address < memory.length) {
            byte power = memory[address];
            byte absPower = (byte) Math.abs(power);
            if (power < 0) {
                System.out.println("   " + address + ": " + absPower + "=" + UIO.chunkLength(absPower) + " allocated=" + UIO.bytesInt(memory, address + 1));
            } else {
                System.out.println(
                    "   " + address + ": " + absPower + "=" + UIO.chunkLength(absPower) + " free prior:" + getPrior(address) + " next:" + getNext(address));
            }
            if (absPower == 0) {
                System.out.println("CORRUPTION");
                System.exit(1);
                break;
            }
            address += UIO.chunkLength(absPower);
        }

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

}
