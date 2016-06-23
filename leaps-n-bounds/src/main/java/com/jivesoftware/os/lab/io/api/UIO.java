/*
 * UIO.java.java
 *
 * Created on 03-12-2010 11:24:38 PM
 *
 * Copyright 2010 Jonathan Colt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.lab.io.api;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.Iterator;

public class UIO {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private UIO() {
    }

    /**
     *
     * @param _filer
     * @param bytes
     * @param fieldName
     * @throws IOException
     */
    public static void write(IAppendOnly _filer, byte[] bytes, String fieldName) throws IOException {
        _filer.append(bytes, 0, bytes.length);
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeByte(IAppendOnly _filer, byte v,
        String fieldName) throws IOException {
        _filer.append(new byte[]{v}, 0, 1);
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeInt(IAppendOnly _filer, int v, String fieldName, byte[] intBuffer) throws IOException {
        intBuffer[0] = (byte) (v >>> 24);
        intBuffer[1] = (byte) (v >>> 16);
        intBuffer[2] = (byte) (v >>> 8);
        intBuffer[3] = (byte) (v);

        _filer.append(intBuffer, 0, 4);
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeLong(IAppendOnly _filer, long v,
        String fieldName) throws IOException {
        _filer.append(new byte[]{
            (byte) (v >>> 56),
            (byte) (v >>> 48),
            (byte) (v >>> 40),
            (byte) (v >>> 32),
            (byte) (v >>> 24),
            (byte) (v >>> 16),
            (byte) (v >>> 8),
            (byte) v
        }, 0, 8);
    }

    /**
     *
     * @param _filer
     * @param l
     * @throws IOException
     */
    private static void writeLength(IAppendOnly _filer, int l, byte[] lengthBuffer) throws IOException {
        writeInt(_filer, l, "length", lengthBuffer);
    }

    public static void writeByteArray(IAppendOnly _filer, byte[] array, String fieldName, byte[] lengthBuffer) throws IOException {
        writeByteArray(_filer, array, 0, array == null ? -1 : array.length, fieldName, lengthBuffer);
    }

    /**
     *
     * @param _filer
     * @param array
     * @param _start
     * @param _len
     * @param fieldName
     * @throws IOException
     */
    public static void writeByteArray(IAppendOnly _filer, byte[] array,
        int _start, int _len, String fieldName, byte[] lengthBuffer) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = _len;
        }
        writeLength(_filer, len, lengthBuffer);
        if (len < 0) {
            return;
        }
        _filer.append(array, _start, len);
    }

    public static void writeLongArray(IAppendOnly _filer, long[] array,
        String fieldName, byte[] lengthBuffer) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len, lengthBuffer);
        if (len < 0) {
            return;
        }
        byte[] bytes = new byte[8 * len];
        for (int i = 0; i < len; i++) {
            long v = array[i];
            UIO.longBytes(v, bytes, i * 8);
        }
        _filer.append(bytes, 0, bytes.length);

    }

    public static long[] readLongArray(IReadable _filer, String fieldName, byte[] lengthBuffer) throws IOException {
        int len = readLength(_filer, lengthBuffer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new long[0];
        }
        long[] array = new long[len];
        byte[] bytes = new byte[8 * len];
        _filer.read(bytes);
        int j;
        for (int i = 0; i < len; i++) {
            j = i * 8;
            long v = 0;
            v |= (bytes[j + 0] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 1] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 2] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 3] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 4] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 5] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 6] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 7] & 0xFF);
            array[i] = v;
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @return
     * @throws IOException
     */
    public static int readLength(IReadable _filer, byte[] lengthBuffer) throws IOException {
        return readInt(_filer, "length", lengthBuffer);
    }

    public static int readLength(byte[] array, int _offset) throws IOException {
        return UIO.bytesInt(array, _offset);
    }

    /**
     *
     * @param _filer
     * @param array
     * @throws IOException
     */
    public static void read(IReadable _filer, byte[] array) throws IOException {
        readFully(_filer, array, array.length);
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static byte[] readByteArray(IReadable _filer, String fieldName, byte[] lengthBuffer) throws IOException {
        int len = readLength(_filer, lengthBuffer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new byte[0];
        }
        byte[] array = new byte[len];
        readFully(_filer, array, len);
        return array;
    }

    public static byte[] readByteArray(byte[] bytes, int _offset, String fieldName) throws IOException {
        int len = readLength(bytes, _offset);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new byte[0];
        }
        byte[] array = new byte[len];
        readFully(bytes, _offset + 4, array, len);
        return array;
    }

    // Reading
    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static boolean readBoolean(IReadable _filer, String fieldName) throws IOException {
        int v = _filer.read();
        if (v < 0) {
            throw new EOFException();
        }
        return (v != 0);
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static byte readByte(IReadable _filer, String fieldName) throws IOException {
        int v = _filer.read();
        if (v < 0) {
            throw new EOFException();
        }
        return (byte) v;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static int readInt(IReadable _filer, String fieldName, byte[] intBuffer) throws IOException {
        readFully(_filer, intBuffer, 4);
        int v = 0;
        v |= (intBuffer[0] & 0xFF);
        v <<= 8;
        v |= (intBuffer[1] & 0xFF);
        v <<= 8;
        v |= (intBuffer[2] & 0xFF);
        v <<= 8;
        v |= (intBuffer[3] & 0xFF);
        return v;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static long readLong(IReadable _filer, String fieldName, byte[] longBuffer) throws IOException {
        readFully(_filer, longBuffer, 8);
        long v = 0;
        v |= (longBuffer[0] & 0xFF);
        v <<= 8;
        v |= (longBuffer[1] & 0xFF);
        v <<= 8;
        v |= (longBuffer[2] & 0xFF);
        v <<= 8;
        v |= (longBuffer[3] & 0xFF);
        v <<= 8;
        v |= (longBuffer[4] & 0xFF);
        v <<= 8;
        v |= (longBuffer[5] & 0xFF);
        v <<= 8;
        v |= (longBuffer[6] & 0xFF);
        v <<= 8;
        v |= (longBuffer[7] & 0xFF);
        return v;
    }

    public static boolean bytesBoolean(byte[] bytes, int _offset) {
        if (bytes == null) {
            return false;
        }
        return bytes[_offset] != 0;
    }

    /**
     *
     * @param v
     * @return
     */
    public static byte[] intBytes(int v) {
        return intBytes(v, new byte[4], 0);
    }

    /**
     *
     * @param v
     * @param _bytes
     * @param _offset
     * @return
     */
    public static byte[] intBytes(int v, byte[] _bytes, int _offset) {
        _bytes[_offset + 0] = (byte) (v >>> 24);
        _bytes[_offset + 1] = (byte) (v >>> 16);
        _bytes[_offset + 2] = (byte) (v >>> 8);
        _bytes[_offset + 3] = (byte) v;
        return _bytes;
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static int bytesInt(byte[] _bytes) {
        return bytesInt(_bytes, 0);
    }

    /**
     *
     * @param v
     * @param _bytes
     * @param _offset
     * @return
     */
    public static byte[] shortBytes(short v, byte[] _bytes, int _offset) {
        _bytes[_offset + 0] = (byte) (v >>> 8);
        _bytes[_offset + 1] = (byte) v;
        return _bytes;
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static short bytesShort(byte[] _bytes) {
        return bytesShort(_bytes, 0);
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static int[] bytesInts(byte[] _bytes) {
        if (_bytes == null || _bytes.length == 0) {
            return null;
        }
        int intsCount = _bytes.length / 4;
        int[] ints = new int[intsCount];
        for (int i = 0; i < intsCount; i++) {
            ints[i] = bytesInt(_bytes, i * 4);
        }
        return ints;
    }

    /**
     *
     * @param bytes
     * @param _offset
     * @return
     */
    public static int bytesInt(byte[] bytes, int _offset) {
        int v = 0;
        v |= (bytes[_offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 1] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 2] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 3] & 0xFF);
        return v;
    }

    /**
     *
     * @param bytes
     * @param _offset
     * @return
     */
    public static short bytesShort(byte[] bytes, int _offset) {
        short v = 0;
        v |= (bytes[_offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 1] & 0xFF);
        return v;
    }

    /**
     *
     * @param _longs
     * @return
     */
    public static byte[] longsBytes(long[] _longs) {
        int len = _longs.length;
        byte[] bytes = new byte[len * 8];
        for (int i = 0; i < len; i++) {
            longBytes(_longs[i], bytes, i * 8);
        }
        return bytes;
    }

    /**
     *
     * @param _v
     * @return
     */
    public static byte[] longBytes(long _v) {
        return longBytes(_v, new byte[8], 0);
    }

    /**
     *
     * @param v
     * @param _bytes
     * @param _offset
     * @return
     */
    public static byte[] longBytes(long v, byte[] _bytes, int _offset) {
        _bytes[_offset + 0] = (byte) (v >>> 56);
        _bytes[_offset + 1] = (byte) (v >>> 48);
        _bytes[_offset + 2] = (byte) (v >>> 40);
        _bytes[_offset + 3] = (byte) (v >>> 32);
        _bytes[_offset + 4] = (byte) (v >>> 24);
        _bytes[_offset + 5] = (byte) (v >>> 16);
        _bytes[_offset + 6] = (byte) (v >>> 8);
        _bytes[_offset + 7] = (byte) v;
        return _bytes;
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static long bytesLong(byte[] _bytes) {
        return bytesLong(_bytes, 0);
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static long[] bytesLongs(byte[] _bytes) {
        if (_bytes == null || _bytes.length == 0) {
            return null;
        }
        int longsCount = _bytes.length / 8;
        long[] longs = new long[longsCount];
        for (int i = 0; i < longsCount; i++) {
            longs[i] = bytesLong(_bytes, i * 8);
        }
        return longs;
    }

    /**
     *
     * @param bytes
     * @param _offset
     * @return
     */
    public static long bytesLong(byte[] bytes, int _offset) {
        if (bytes == null) {
            return 0;
        }
        long v = 0;
        v |= (bytes[_offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 1] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 2] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 3] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 4] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 5] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 6] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 7] & 0xFF);
        return v;
    }

    /**
     *
     * @param length
     * @param _minPower
     * @return
     */
    public static int chunkPower(long length, int _minPower) {
        if (length == 0) {
            return 0;
        }
        int numberOfTrailingZeros = Long.numberOfLeadingZeros(length - 1);
        return Math.max(_minPower, 64 - numberOfTrailingZeros);
    }

    /**
     *
     * @param _chunkPower
     * @return
     */
    public static long chunkLength(int _chunkPower) {
        return 1L << _chunkPower;
    }

    public static int writeBytes(byte[] value, byte[] destination, int offset) {
        if (value != null) {
            System.arraycopy(value, 0, destination, offset, value.length);
            return value.length;
        }
        return 0;
    }

    public static void readBytes(byte[] source, int offset, byte[] value) {
        System.arraycopy(source, offset, value, 0, value.length);
    }

    public static void readFully(IReadable readable, byte[] into, int length) throws IOException {
        int read = readable.read(into, 0, length);
        if (read != length) {
            throw new IOException("Failed to fully. Only had " + read + " needed " + length);
        }
    }

    private static void readFully(byte[] from, int offset, byte[] into, int length) throws IOException {
        System.arraycopy(from, offset, into, 0, length);
    }

    /**
     * Lex key range splittting Copied from HBase
     * Iterate over keys within the passed range.
     */
    public static Iterable<byte[]> iterateOnSplits(byte[] a, byte[] b, boolean inclusive, int num, Comparator<byte[]> lexicographicalComparator) {
        byte[] aPadded;
        byte[] bPadded;
        if (a.length < b.length) {
            aPadded = padTail(a, b.length - a.length);
            bPadded = b;
        } else if (b.length < a.length) {
            aPadded = a;
            bPadded = padTail(b, a.length - b.length);
        } else {
            aPadded = a;
            bPadded = b;
        }
        if (lexicographicalComparator.compare(aPadded, bPadded) >= 0) {
            throw new IllegalArgumentException("b <= a");
        }
        if (num <= 0) {
            throw new IllegalArgumentException("num cannot be <= 0");
        }
        byte[] prependHeader = {1, 0};
        BigInteger startBI = new BigInteger(add(prependHeader, aPadded));
        BigInteger stopBI = new BigInteger(add(prependHeader, bPadded));
        BigInteger diffBI = stopBI.subtract(startBI);
        if (inclusive) {
            diffBI = diffBI.add(BigInteger.ONE);
        }
        BigInteger splitsBI = BigInteger.valueOf(num + 1);
        //when diffBI < splitBI, use an additional byte to increase diffBI
        if (diffBI.compareTo(splitsBI) < 0) {
            byte[] aPaddedAdditional = new byte[aPadded.length + 1];
            byte[] bPaddedAdditional = new byte[bPadded.length + 1];
            System.arraycopy(aPadded, 0, aPaddedAdditional, 0, aPadded.length);
            System.arraycopy(bPadded, 0, bPaddedAdditional, 0, bPadded.length);
            aPaddedAdditional[aPadded.length] = 0;
            bPaddedAdditional[bPadded.length] = 0;
            return iterateOnSplits(aPaddedAdditional, bPaddedAdditional, inclusive, num, lexicographicalComparator);
        }
        BigInteger intervalBI;
        try {
            intervalBI = diffBI.divide(splitsBI);
        } catch (Exception e) {
            LOG.error("Exception caught during division", e);
            return null;
        }

        Iterator<byte[]> iterator = new Iterator<byte[]>() {
            private int i = -1;

            @Override
            public boolean hasNext() {
                return i < num + 1;
            }

            @Override
            public byte[] next() {
                i++;
                if (i == 0) {
                    return a;
                }
                if (i == num + 1) {
                    return b;
                }

                BigInteger curBI = startBI.add(intervalBI.multiply(BigInteger.valueOf(i)));
                byte[] padded = curBI.toByteArray();
                if (padded[1] == 0) {
                    padded = tail(padded, padded.length - 2);
                } else {
                    padded = tail(padded, padded.length - 1);
                }
                return padded;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };

        return () -> iterator;
    }

    /**
     * @param a array
     * @param length new array size
     * @return Value in <code>a</code> plus <code>length</code> appended 0 bytes
     */
    public static byte[] padTail(byte[] a, int length) {
        byte[] padding = new byte[length];
        for (int i = 0; i < length; i++) {
            padding[i] = 0;
        }
        return add(a, padding);
    }

    /**
     * @param a lower half
     * @param b upper half
     * @return New array that has a in lower half and b in upper half.
     */
    public static byte[] add(byte[] a, byte[] b) {
        byte[] result = new byte[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    /**
     * @param a array
     * @param length amount of bytes to snarf
     * @return Last <code>length</code> bytes from <code>a</code>
     */
    public static byte[] tail(final byte[] a, final int length) {
        if (a.length < length) {
            return null;
        }
        byte[] result = new byte[length];
        System.arraycopy(a, a.length - length, result, 0, length);
        return result;
    }
}
