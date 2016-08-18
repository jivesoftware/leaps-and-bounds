package com.jivesoftware.os.lab.guts;

import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedLongs;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.io.api.IReadable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import sun.misc.Unsafe;

/**
 * @author jonathan.colt
 */
public class IndexUtil {

    public static boolean equals(ByteBuffer a, ByteBuffer b) {
        if (a == null && b == null) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        a.clear();
        b.clear();
        return a.equals(b);
    }

    public static String toString(ByteBuffer bb) {
        return Arrays.toString(toByteArray(bb));
    }

    public static byte[] toByteArray(ByteBuffer bb) {
        if (bb == null) {
            return null;
        }
        bb.clear();
        byte[] array = new byte[bb.capacity()];
        bb.get(array);
        return array;
    }

    private static class PointGetRaw implements GetRaw {

        private final GetRaw[] pointGets;
        private boolean result;

        public PointGetRaw(GetRaw[] pointGets) {
            this.pointGets = pointGets;
        }

        @Override
        public boolean get(byte[] key, RawEntryStream stream) throws Exception {
            for (GetRaw pointGet : pointGets) {
                if (pointGet.get(key, stream)) {
                    result = pointGet.result();
                    return result;
                }
            }
            result = stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, null);
            return result;
        }

        @Override
        public boolean result() {
            return result;
        }
    }

    public static GetRaw get(ReadIndex[] indexes) throws Exception {
        GetRaw[] pointGets = new GetRaw[indexes.length];
        for (int i = 0; i < pointGets.length; i++) {
            pointGets[i] = indexes[i].get();
        }
        return new PointGetRaw(pointGets);
    }

    public static NextRawEntry rangeScan(ReadIndex[] copy, byte[] from, byte[] to, Rawhide rawhide) throws Exception {
        NextRawEntry[] feeders = new NextRawEntry[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].rangeScan(from, to);
        }
        return new InterleaveStream(feeders, rawhide);
    }

    public static NextRawEntry rowScan(ReadIndex[] copy, Rawhide rawhide) throws Exception {
        NextRawEntry[] feeders = new NextRawEntry[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].rowScan();
        }
        return new InterleaveStream(feeders, rawhide);
    }

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

    public static int compare(IReadable left, int leftLength, byte[] right, int rightOffset, int rightLength) throws IOException {
        int minLength = Math.min(leftLength, rightLength);
        for (int i = 0; i < minLength; i++) {
            int result = (left.read() & 0xFF) - (right[rightOffset + i] & 0xFF);
            if (result != 0) {
                return result;
            }
        }
        return leftLength - rightLength;
    }

    public static int compare(IReadable left, int leftLength, ByteBuffer right) throws IOException {
        int rightLength = right.capacity();
        int minLength = Math.min(leftLength, rightLength);
        for (int i = 0; i < minLength; i++) {
            int result = (left.read() & 0xFF) - (right.get(i) & 0xFF);
            if (result != 0) {
                return result;
            }
        }
        return leftLength - rightLength;
    }

    public static int compare(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        if (theUnsafe != null) {
            return compareNative(left, leftOffset, leftLength, right, rightOffset, rightLength);
        } else {
            return comparePure(left, leftOffset, leftLength, right, rightOffset, rightLength);
        }
    }

    private static int compareNative(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        int minLength = Math.min(leftLength, rightLength);
        int minWords = minLength / 8;

        int i;
        for (i = 0; i < minWords * 8; i += 8) {
            long result = theUnsafe.getLong(left, (long) BYTE_ARRAY_BASE_OFFSET + leftOffset + i);
            long rw = theUnsafe.getLong(right, (long) BYTE_ARRAY_BASE_OFFSET + rightOffset + i);
            if (result != rw) {
                if (BIG_ENDIAN) {
                    return UnsignedLongs.compare(result, rw);
                }

                int n = Long.numberOfTrailingZeros(result ^ rw) & -8;
                return (int) ((result >>> n & 255L) - (rw >>> n & 255L));
            }
        }

        for (i = minWords * 8; i < minLength; ++i) {
            int var11 = UnsignedBytes.compare(left[leftOffset + i], right[rightOffset + i]);
            if (var11 != 0) {
                return var11;
            }
        }

        return leftLength - rightLength;
    }

    private static int comparePure(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        int minLength = Math.min(leftLength, rightLength);
        for (int i = 0; i < minLength; i++) {
            int result = (left[leftOffset + i] & 0xFF) - (right[rightOffset + i] & 0xFF);
            if (result != 0) {
                return result;
            }
        }
        return leftLength - rightLength;
    }


    public static int compare(ByteBuffer left, ByteBuffer right) {
        int leftLength = left.capacity();
        int rightLength = right.capacity();

        int minLength = Math.min(leftLength, rightLength);
        int minWords = minLength / 8;

        int i;
        for (i = 0; i < minWords * 8; i += 8) {
            long result = left.getLong(i);
            long rw = right.getLong(i);
            if (result != rw) {
                return UnsignedLongs.compare(result, rw);
            }
        }

        for (i = minWords * 8; i < minLength; ++i) {
            int var11 = UnsignedBytes.compare(left.get(i), right.get(i));
            if (var11 != 0) {
                return var11;
            }
        }

        return leftLength - rightLength;
    }
}
