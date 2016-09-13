package com.jivesoftware.os.lab.guts.allocators;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import sun.misc.Unsafe;

/**
 *
 * @author jonathan.colt
 */
public class LABCASBuddyAllocator implements LABMemoryAllocator {

    /**
     * Borrowed from guava.
     */
    static final boolean BIG_ENDIAN;
    static final Unsafe theUnsafe;
    static final int BYTE_ARRAY_BASE_OFFSET;

    static {
        BIG_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
        theUnsafe = getUnsafe();
        if (theUnsafe != null) {
            BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
            if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
                throw new AssertionError();
            }
        } else {
            BYTE_ARRAY_BASE_OFFSET = -1;
        }
    }

    private static Unsafe getUnsafe() {
        try {
            return Unsafe.getUnsafe();
        } catch (SecurityException var2) {
            try {
                return (Unsafe) AccessController.doPrivileged((PrivilegedExceptionAction) () -> {
                    Class k = Unsafe.class;
                    Field[] arr$ = k.getDeclaredFields();
                    int len$ = arr$.length;

                    for (int i$ = 0; i$ < len$; ++i$) {
                        Field f = arr$[i$];
                        f.setAccessible(true);
                        Object x = f.get(null);
                        if (k.isInstance(x)) {
                            return (Unsafe) k.cast(x);
                        }
                    }
                    return null;
                });
            } catch (PrivilegedActionException var1) {
                return null;
            }
        } catch (Throwable t) {
            return null;
        }
    }

//    private byte getByte(long address) {
//        return theUnsafe.getByteVolatile(memory, BYTE_ARRAY_BASE_OFFSET + address);
//    }
//
//    private void setByte(long address, byte value) {
//        theUnsafe.putByteVolatile(memory, BYTE_ARRAY_BASE_OFFSET + address, value);
//    }
//
//    private int getInt(long address) {
//        return theUnsafe.getIntVolatile(memory, BYTE_ARRAY_BASE_OFFSET + address);
//    }
//
//    private void setInt(long address, int value) {
//        theUnsafe.putIntVolatile(memory, BYTE_ARRAY_BASE_OFFSET + address, value);
//    }
//
//    private boolean casInt(long address, int expected, int desired) {
//        return theUnsafe.compareAndSwapInt(memory, BYTE_ARRAY_BASE_OFFSET + address, expected, desired);
//    }
    private byte getByte(long address) {
        return memory[(int) address];
    }

    private void setByte(long address, byte value) {
        memory[(int) address] = value;
    }

    private int getInt(long address) {
        return UIO.bytesInt(memory, (int) address);
    }

    private void setInt(long address, int value) {
        UIO.intBytes(value, memory, (int) address);
    }

    private boolean casInt(long address, int expected, int desired) {
        //synchronized (memory) {
        int current = UIO.bytesInt(memory, (int) address);
        if (current == expected) {
            UIO.intBytes(desired, memory, (int) address);
            return true;
        }
        return false;
        //}
    }

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final int MERGED = -8;
    private static final int SPLITTING = -7;
    private static final int COLLECTING = -6;
    private static final int ATTACHING = -5;
    private static final int MERGING_LEFT = -4;
    private static final int MERGING_RIGHT = -3;
    private static final int ALLOCATING = -2;
    private static final int DETACHING = -1;
    private static final int FREE = 0;
    private static final int ALLOCATED = 1;

    private final String name;
    private final byte minPowerSize;
    private final byte maxPowerSize;
    volatile byte[] memory;
    private final Callable<Void> calledOnFull;
    private final AtomicIntegerArray freePowerAddress;
    private final AtomicIntegerArray freePowerLocks;

    public static final AtomicLong allocatations = new AtomicLong();
    public static final AtomicLong releases = new AtomicLong();
    public static final AtomicLong merged = new AtomicLong();
    public static final AtomicLong split = new AtomicLong();

    public LABCASBuddyAllocator(String name, byte minPowerSize, byte maxPowerSize, Callable<Void> calledOnFull) {
        Preconditions.checkArgument(minPowerSize > 2);
        Preconditions.checkArgument(maxPowerSize < 32);

        this.name = name;
        this.minPowerSize = minPowerSize;
        this.maxPowerSize = maxPowerSize;
        this.memory = new byte[(int) UIO.chunkLength(maxPowerSize)];
        this.calledOnFull = calledOnFull;
        int[] freePowerAddressArray = new int[maxPowerSize + 1];
        Arrays.fill(freePowerAddressArray, -1);
        freePowerAddressArray[maxPowerSize] = 0;
        this.freePowerAddress = new AtomicIntegerArray(freePowerAddressArray);
        this.freePowerLocks = new AtomicIntegerArray(maxPowerSize + 1);
        forceReference(0, 0);
        setPower(0, maxPowerSize);
        setNext(0, -1);
        setPrior(0, -1);
    }

    @Override
    public long sizeInBytes() {
        throw new UnsupportedOperationException("TODO.");
    }    

    @Override
    public byte[] bytes(long address) throws InterruptedException {
        synchronized (this) {
            if (acquire(address)) {
                try {
                    long a = address + 1 + 4;
                    int length = getInt(a);
                    byte[] copy = new byte[length];
                    System.arraycopy(memory, (int) a + 4, copy, 0, length);
                    return copy;
                } finally {
                    release(address);
                }
            } else {
                return null;
            }
        }
    }

    // must call release
    @Override
    public boolean acquireBytes(long address, BolBuffer bolBuffer) throws InterruptedException {
        synchronized (this) {
            if (acquire(address)) {
                bolBuffer.bytes = memory;
                bolBuffer.offset = (int) (address + 1 + 4 + 4);
                bolBuffer.length = getInt(address + 1 + 4);
                return true;
            } else {
                return false;
            }
        }
    }

    // must call release
    public long allocate(byte[] bytes, int offset, int length) throws InterruptedException {

        if (bytes == null) {
            throw new IllegalStateException();
        }
        int desiredPower = UIO.chunkPower(1 + 4 + 4 + length, minPowerSize);

        AGAIN:
        while (true) {
            synchronized (this) {
                int allocate = allocate(desiredPower);
                if (allocate != -1) {
                    copy(allocate, bytes, offset, length);

                    allocatations.incrementAndGet();
                    return allocate;
                }

                for (int power = desiredPower + 1; power <= maxPowerSize; power++) {

                    allocate = allocate(power);
                    if (allocate != -1) {

                        int splitPower = power - 1;

                        int buddyAddress = allocate + (int) UIO.chunkLength(splitPower);

                        forceReference(buddyAddress, SPLITTING);
                        free(splitPower, buddyAddress);
                        free(splitPower, allocate);
                        split.incrementAndGet();
                        continue AGAIN;
                    }
                }
            }

            try {
                calledOnFull.call();
            } catch (Exception x) {
                LOG.warn("notifyAllOnFull barfed", x);
            }
            yield();
        }
    }

    private int allocate(final int power) throws InterruptedException {
        while (true) {
            if (grabPower(power)) {
                int address = freePowerAddress.get(power);
                if (address == -1) {
                    letGoOfPower(power);
                    return -1;
                } else if (grab(address, FREE, ALLOCATING)) {
                    int latestPower = getPower(address);
                    if (power != latestPower) {
                        letGo(address, ALLOCATING, FREE);
                    } else {
                        int status = detach(power, address, ALLOCATING, ALLOCATING, FREE);
                        if (status >= DETACHED_ONLY_ONE) {
                            if (!grab(address, ALLOCATING, ALLOCATED)) {
                                throw new IllegalStateException("Should be impossible.");
                            }
                            letGoOfPower(power);
                            return address;
                        }
                    }
                }
                letGoOfPower(power);
            }
            yield();
        }
    }

    private static final int DETACHED_HEAD = 4;
    private static final int DETACHED_TAIL = 3;
    private static final int DETACHED_MIDDLE = 2;
    private static final int DETACHED_ONLY_ONE = 1;
    private static final int NEXT = -1;
    private static final int PRIOR = -2;
    private static final int MIDDLE = -3;

    private int detach(int power, long middleAddress, int expected, int success, int failure) throws InterruptedException {
        if (grab(middleAddress, expected, DETACHING)) {
            int priorAddress = getPrior((int) middleAddress);
            if (priorAddress != -1) {
                if (grab(priorAddress, FREE, DETACHING)) {
                    int nextAddress = getNext((int) middleAddress);
                    if (nextAddress != -1) { // middle
                        if (grab(nextAddress, FREE, DETACHING)) {
                            setNext(priorAddress, nextAddress);
                            setPrior(nextAddress, priorAddress);
                            letGo(priorAddress, DETACHING, FREE);
                            letGo(middleAddress, DETACHING, success);
                            letGo(nextAddress, DETACHING, FREE);
                            return DETACHED_MIDDLE;
                        } else {
                            letGo(priorAddress, DETACHING, FREE);
                            letGo(middleAddress, DETACHING, failure);
                            return NEXT;
                        }
                    } else { // tail
                        setNext(priorAddress, -1);
                        letGo(priorAddress, DETACHING, FREE);
                        letGo(middleAddress, DETACHING, success);
                        return DETACHED_TAIL;
                    }
                } else {
                    letGo(middleAddress, DETACHING, failure);
                    return PRIOR;
                }
            } else { // head
                int nextAddress = getNext((int) middleAddress);
                if (nextAddress != -1) { // more than one
                    if (grab(nextAddress, FREE, DETACHING)) {
                        clearPrior(middleAddress);
                        clearNext(middleAddress);
                        clearPrior(nextAddress);
                        freePowerAddress.set(power, nextAddress);

                        letGo(middleAddress, DETACHING, success);
                        letGo(nextAddress, DETACHING, FREE);
                        return DETACHED_HEAD;
                    } else {
                        letGo(middleAddress, DETACHING, failure);
                        return NEXT;
                    }
                } else { // only one
                    clearPrior(middleAddress);
                    clearNext(middleAddress);
                    freePowerAddress.set(power, -1);
                    letGo(middleAddress, DETACHING, success);
                    return DETACHED_ONLY_ONE;
                }
            }
        }
        return MIDDLE;
    }

    private void free(final int power, int freeAddress) throws InterruptedException {
        while (true) {
            if (grabPower(power)) {

                int address = freePowerAddress.get(power);
                if (address == -1) {
                    clearPrior(freeAddress);
                    clearNext(freeAddress);
                    setPower(freeAddress, (byte) power);
                    if (freePowerAddress.compareAndSet(power, -1, freeAddress)) {
                        forceFree(freeAddress, power);
                        letGoOfPower(power);
                        break;
                    } else {
                        throw new IllegalStateException("Should be impossible. Freeing power:" + power + " address:" + address);
                    }
                } else if (address > -1) {
                    if (grab(address, FREE, ATTACHING)) {
                        int latestPower = getPower(address);
                        if (power != latestPower) {
                            letGo(address, ATTACHING, FREE);
                        } else {
                            clearPrior(freeAddress);
                            clearNext(freeAddress);
                            setPower(freeAddress, (byte) power);
                            setNext(freeAddress, address);
                            setPrior(address, freeAddress);
                            letGo(address, ATTACHING, FREE);
                            if (freePowerAddress.compareAndSet(power, address, freeAddress)) {
                                forceFree(freeAddress, power);
                                letGoOfPower(power);
                                break;
                            } else {
                                throw new IllegalStateException("Should be impossible. Freeing power:" + power + " address:" + address);
                            }
                        }
                    }
                }
                letGoOfPower(power);
            }
            yield();
        }
    }

    private void forceFree(int freeAddress, int power) {
        setInt(freeAddress + 1, FREE);
    }

    private boolean grabPower(final int power) {
        return freePowerLocks.compareAndSet(power, 0, 1);
    }

    private void letGoOfPower(int power) {
        if (!freePowerLocks.compareAndSet(power, 1, 0)) {
            throw new IllegalStateException("letGoOfPower:" + power);
        }
    }

    // 1 power, 4 refCount, 4 prevPointer, 4 nextPointer or 1 power, 4 refCount, 4 length, .....
    // returns false if the address has been collected
    public boolean acquire(long address) throws InterruptedException {

        long i = address + 1;
        while (true) {
            synchronized (this) {
                int count = getInt(i);
                if (count <= 0) {
                    return false;
                }
                if (casInt(i, count, count + 1)) {
                    return true;
                }
            }
            yield();
        }
    }

    public void release(long address) throws InterruptedException {

        long i = address + 1;
        while (true) {
            synchronized (this) {
                int count = getInt(i);
                if (count == 0) {
                    releases.incrementAndGet();
                    return;
                }
                if (casInt(i, count, (count - 1 == 0 ? COLLECTING : count - 1))) {
                    if (count - 1 == 0) {
                        collect(address);
                    }
                    releases.incrementAndGet();
                    return;
                }
            }
            yield();
        }

    }

    // currently only called by release. Its expected the free address is detached.
    private void collect(long freeAddress) throws InterruptedException {

        int address = (int) freeAddress;
        byte power = getPower(address);
        byte absPower = (byte) Math.abs(power);
        setPower(address, absPower);

        while (true) {
            long chunkLength = UIO.chunkLength(absPower);

            if (((address / chunkLength) & 1) == 0) { // % 2
                int nextAddress = (int) (address + chunkLength);
                if (nextAddress >= memory.length) {
                    break;
                }
                int grabPower = absPower;
                if (grabPower(grabPower)) {
                    if (grab(nextAddress, FREE, MERGING_RIGHT)) {
                        if (getPower(nextAddress) == absPower) {
                            if (detach(grabPower, nextAddress, MERGING_RIGHT, MERGING_RIGHT, FREE) >= DETACHED_ONLY_ONE) {
                                absPower = (byte) (absPower + 1);
                                setPower(address, absPower);
                                merged.incrementAndGet();
                            }
                            letGoOfPower(grabPower);
                        } else {
                            letGo(nextAddress, MERGING_RIGHT, FREE);
                            letGoOfPower(grabPower);
                            break;
                        }
                    } else if (refereceCount(nextAddress) < ALLOCATED) {
                        letGoOfPower(grabPower);
                        // fall through to the yield
                    } else {
                        letGoOfPower(grabPower);
                        break;
                    }
                }
            } else if (getPower(address - chunkLength) == absPower && refereceCount(address - chunkLength) == FREE) {
                if ((address - chunkLength) < 0) {
                    break;
                }
                int priorAddress = (int) (address - chunkLength);
                int grabPower = absPower;
                if (grabPower(grabPower)) {
                    if (grab(priorAddress, FREE, MERGING_LEFT)) {
                        if (getPower(priorAddress) == absPower) {
                            if (detach(grabPower, priorAddress, MERGING_LEFT, MERGING_LEFT, FREE) >= DETACHED_ONLY_ONE) {
                                absPower = (byte) (absPower + 1);
                                setPower(priorAddress, absPower);
                                merged.incrementAndGet();
                                address = priorAddress;
                            }
                            letGoOfPower(grabPower);
                        } else {
                            letGo(priorAddress, MERGING_LEFT, FREE);
                            letGoOfPower(grabPower);
                            break;
                        }
                    } else if (refereceCount(priorAddress) < ALLOCATED) {
                        letGoOfPower(grabPower);
                        // fall through to the yield
                    } else {
                        letGoOfPower(grabPower);
                        break;
                    }
                }
            } else {
                break;
            }
            yield();
        }
        free(absPower, address);

    }

    private boolean grab(long address, int expected, int desired) throws InterruptedException {
        return casInt(address + 1, expected, desired);
    }

    private void letGo(long address, int expected, int desired) throws InterruptedException {
        if (!casInt(address + 1, expected, desired)) {
            throw new IllegalStateException("We failed expected:" + expected + " desired:" + desired + " was:" + refereceCount((int) address));
        }
    }

    private int refereceCount(long address) {
        return getInt(address + 1);
    }

    private void forceReference(long address, int value) {
        setInt(address + 1, value);
    }

    private void yield() throws InterruptedException {
        Thread.yield();
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
    }

    private void copy(int address, byte[] bytes, int offset, int length) {
        allocatedPower(address); // grr
        setInt(address + 1 + 4, length);
        System.arraycopy(bytes, offset, memory, address + 1 + 4 + 4, length);
    }

    private void allocatedPower(long address) {
        setByte(address, (byte) -Math.abs(getByte(address)));
    }

    private void setPower(long address, byte power) {
        setByte(address, power);
    }

    private byte getPower(long address) {
        return getByte(address);
    }

    private int getPrior(long address) {
        if (address == -1) {
            return -1;
        }
        return getInt(address + 1 + 4);
    }

    private void setPrior(long address, int prior) {
        if (address == -1 || address == prior) {
            throw new IllegalArgumentException("address:" + address + " prior:" + prior);
        }
        setInt(address + 1 + 4, prior);
    }

    private void clearPrior(long address) {
        if (address == -1) {
            return;
        }
        setInt(address + 1 + 4, -1);
    }

    private int getNext(long address) {
        if (address == -1) {
            return -1;
        }
        return getInt(address + 1 + 4 + 4);
    }

    private void setNext(long address, int next) {
        if (address == -1 || address == next) {
            synchronized (System.out) {
                throw new IllegalArgumentException("address:" + address + " next:" + next);
            }
        }
        setInt(address + 1 + 4 + 4, next);
    }

    private void clearNext(long address) {
        if (address == -1) {
            return;
        }
        setInt(address + 1 + 4 + 4, -1);
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

    public void dump() {
        int address = 0;
        System.out.println("Frees:" + freePowerAddress.toString());
        while (address < memory.length) {
            byte power = memory[address];
            byte absPower = (byte) Math.abs(power);
            if (power < 0) {
                System.out.println(
                    "   " + address + ": " + absPower + "=" + UIO.chunkLength(absPower) + " refCount:" + refereceCount(address) + " allocated=" + UIO.bytesInt(
                    memory, address + 1 + 4));
            } else {
                System.out.println(
                    "   " + address + ": " + absPower + "=" + UIO.chunkLength(absPower) + " refCount:" + refereceCount(address) + " free prior:" + getPrior(
                    address) + " next:" + getNext(address));
            }
            if (absPower == 0) {
                System.out.println("CORRUPTION");
                System.exit(1);
                break;
            }
            address += UIO.chunkLength(absPower);
        }
    }



}
