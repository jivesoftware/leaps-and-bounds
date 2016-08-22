package com.jivesoftware.os.lab;

import java.lang.reflect.Field;
import java.util.Comparator;
import sun.misc.Unsafe;

/**
 *
 * @author jonathan.colt
 */
public class LABIndexableMemory implements Comparator<Long> {

    private static Unsafe unsafe;

    {

        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unsafe = (Unsafe) theUnsafe.get(null);
        } catch (Exception x) {
            System.out.println("NOPE screwed by unsafe");
            System.exit(1);
        }
    }

    private final Comparator<byte[]> comparator;
    //private final Map<Long, byte[]> map = new ConcurrentHashMap<>();

    public LABIndexableMemory(Comparator<byte[]> comparator) {
        this.comparator = comparator;
    }

    public byte[] bytes(long index) {
//        if (!map.containsKey(index)) {
//            throw new IllegalStateException();
//        }
        int size = unsafe.getInt(index);
        byte[] bytes = new byte[size];
        unsafe.copyMemory(null, index + 4, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, bytes.length);
        return bytes;
    }

    public long allocate(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalStateException();
        }


        long index = unsafe.allocateMemory(bytes.length + 4);
        //map.put(index, bytes);
        unsafe.putInt(index, bytes.length);
        unsafe.copyMemory(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, index + 4, bytes.length);
        return index;
    }

    public void free(long index) {
//        if (!map.containsKey(index)) {
//            throw new IllegalStateException();
//        }
//        map.remove(index);

        //int size = unsafe.getInt(index);
        unsafe.freeMemory(index); //, size + 4);
    }

    @Override
    public int compare(Long o1, Long o2) {
        return comparator.compare(bytes(o1), bytes(o2));
    }

    public int compare(Long o1, byte[] o2) {
        return comparator.compare(bytes(o1), o2);
    }

    public int compare(byte[] o1, Long o2) {
        return comparator.compare(o1, bytes(o2));
    }

    public int compare(byte[] o1, byte[] o2) {
        return comparator.compare(o1, o2);
    }

    Comparator<? super byte[]> bytesCompartor() {
        return comparator;
    }

    void freeAll() {
    }
}
