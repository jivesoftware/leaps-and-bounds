package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.lang.reflect.Field;
import java.util.Comparator;
import sun.misc.Unsafe;

/**
 *
 * @author jonathan.colt
 */
public class LABIndexableMemory {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

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
//    private static Memory memory = OS.memory();
    private final Rawhide rawhide;
    private final Comparator<byte[]> comparator;
//    private final AtomicLong pointerProvider = new AtomicLong();
//    private final Map<Long, byte[]> map = new ConcurrentHashMap<>();

    public LABIndexableMemory(Rawhide rawhide) {
        this.rawhide = rawhide;
        this.comparator = rawhide.getKeyComparator();
    }

    public byte[] bytes(long index) {
        if (index == -1) {
            return null;
        }

//        int size = memory.readInt(index);
//        byte[] bytes = new byte[size];
//        memory.copyMemory(index + 4, bytes, 0, bytes.length);
//        return bytes;
        int size = unsafe.getInt(index);
        byte[] bytes = new byte[size];
        unsafe.copyMemory(null, index + 4, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, bytes.length);
        //return bytes;

//        if (!map.containsKey(index)) {
//            throw new IllegalStateException();
//        }
//
//        byte[] got = map.get(index);
//        //return bytes;
//
//
//        for (int i = 0; i < got.length; i++) {
//            if (got[i] != bytes[i]) {
//                throw new IllegalStateException(Arrays.toString(bytes) + " vs " + Arrays.toString(got));
//            }
//
//        }
//
//        byte[] wtf = new byte[bytes.length];
//        System.arraycopy(bytes, 0, wtf, 0, bytes.length);
        return bytes;
    }

    public long allocate(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalStateException();
        }
        LOG.incAtomic("allocated");

//        long index = memory.allocate(bytes.length + 4);
//        memory.writeInt(index, bytes.length);
//        memory.copyMemory(bytes, 0, index + 4, bytes.length);
//
//        return index;
        long index = unsafe.allocateMemory(bytes.length + 4);
        unsafe.putInt(index, bytes.length);
        unsafe.copyMemory(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, index + 4, bytes.length);
//        return index;

//        long index = pointerProvider.incrementAndGet();
//        map.put(index, bytes);
        return index;
    }

    public void free(long index) {
        if (index == -1) {
            return;
        }
        LOG.decAtomic("allocated");

//        int size = memory.readInt(index);
//        memory.freeMemory(index, size + 4);
//        if (!map.containsKey(index)) {
//            throw new IllegalStateException();
//        }
//        map.remove(index);
        unsafe.freeMemory(index);
    }

    public int compare(long o1, long o2) {
        if (o1 == -1 && o2 == -1) {
            return rawhide.compareLL(-1, -1, -1, -1);
        } else if (o1 == -1) {
            int l2 = unsafe.getInt(o2);
            return rawhide.compareLL(-1, -1, o2 + 4, l2);
        } else if (o2 == -1) {
            int l1 = unsafe.getInt(o1);
            return rawhide.compareLL(o1 + 4, l1, -1, -1);
        } else {
            int l1 = unsafe.getInt(o1);
            int l2 = unsafe.getInt(o2);
            return rawhide.compareLL(o1 + 4, l1, o2 + 4, l2);
        }
    }

    public int compare(long o1, byte[] o2) {
        if (o1 == -1) {
            return rawhide.compareLB(-1, -1, o2, 0, o2 == null ? -1 : o2.length);
        } else {
            int l1 = unsafe.getInt(o1);
            return rawhide.compareLB(o1 + 4, l1, o2, 0, o2 == null ? -1 : o2.length);
        }
    }

    public int compare(byte[] o1, long o2) {
        if (o2 == -1) {
            return rawhide.compareBL(o1, 0, o1 == null ? -1 : o1.length, -1, -1);
        } else {
            int l2 = unsafe.getInt(o2);
            return rawhide.compareBL(o1, 0, o1 == null ? -1 : o1.length, o2 + 4, l2);
        }
    }

    public int compare(byte[] o1, byte[] o2) {
        return rawhide.compareBB(o1, 0, o1 == null ? -1 : o1.length, o2, 0, o2 == null ? -1 : o2.length);
    }

    Comparator<? super byte[]> bytesComparator() {
        return comparator;
    }

    void freeAll() {
    }
}
