package com.jivesoftware.os.lab.guts;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.io.AppendableHeap;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public class SimpleRawhide implements Rawhide {

    public static String toString(byte[] rawEntry) {
        return "key:" + key(rawEntry) + " value:" + value(rawEntry);
    }

    @Override
    public byte[] merge(FormatTransformer currentReadKeyFormatTransormer,
        FormatTransformer currentReadValueFormatTransormer,
        byte[] currentRawEntry,
        FormatTransformer addingReadKeyFormatTransormer,
        FormatTransformer addingReadValueFormatTransormer,
        byte[] addingRawEntry,
        FormatTransformer mergedReadKeyFormatTransormer,
        FormatTransformer mergedReadValueFormatTransormer) {

        return value(currentRawEntry) > value(addingRawEntry) ? currentRawEntry : addingRawEntry;
    }

    public static long key(byte[] rawEntry) {
        return UIO.bytesLong(rawEntry, 4);
    }

    @Override
    public long timestamp(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        byte[] rawEntry,
        int offset,
        int length) {

        return value(rawEntry);
    }

    @Override
    public long version(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        byte[] rawEntry,
        int offset,
        int length) {

        return -1;
    }

    public static long value(byte[] rawEntry) {
        return UIO.bytesLong(rawEntry, 4 + 8);
    }

    public static byte[] rawEntry(long key, long value) {
        byte[] rawEntry = new byte[4 + 8 + 8];
        UIO.intBytes(8, rawEntry, 0);
        UIO.longBytes(key, rawEntry, 4);
        UIO.longBytes(value, rawEntry, 4 + 8);
        return rawEntry;
    }

    @Override
    public boolean streamRawEntry(int index,
        FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        byte[] rawEntry,
        int offset,
        ValueStream stream,
        boolean hydrateValues) throws Exception {

        if (rawEntry == null) {
            return stream.stream(index, null, -1, false, -1, null);
        }
        int o = offset;
        int keyLength = UIO.bytesInt(rawEntry, o);
        o += 4;
        byte[] k = new byte[keyLength];
        System.arraycopy(rawEntry, o, k, 0, keyLength);
        o += keyLength;
        long timestamp = UIO.bytesLong(rawEntry, o);
        o += 8;
        boolean tombstone = rawEntry[o] != 0;
        o++;
        long version = UIO.bytesLong(rawEntry, o);
        o += 8;

        byte[] value = null;
        if (hydrateValues) {
            int valueLength = UIO.bytesInt(rawEntry, o);
            o += 4;
            value = new byte[valueLength];
            System.arraycopy(rawEntry, o, value, 0, valueLength);
            o += valueLength;
        }
        return stream.stream(index, readKeyFormatTransormer.transform(k), timestamp, tombstone, version, readValueFormatTransormer.transform(value));
    }

    @Override
    public byte[] toRawEntry(byte[] key, long timestamp, boolean tombstoned, long version, byte[] value) throws IOException {

        AppendableHeap indexEntryFiler = new AppendableHeap(4 + key.length + 8 + 1 + 4 + value.length); // TODO somthing better
        byte[] lengthBuffer = new byte[4];
        UIO.writeByteArray(indexEntryFiler, key, "key");
        UIO.writeLong(indexEntryFiler, timestamp, "timestamp");
        UIO.writeByte(indexEntryFiler, tombstoned ? (byte) 1 : (byte) 0, "tombstone");
        UIO.writeLong(indexEntryFiler, version, "version");
        UIO.writeByteArray(indexEntryFiler, value, "value");
        return indexEntryFiler.getBytes();
    }

    @Override
    public int rawEntryLength(IReadable readable) throws Exception {
        int length = UIO.readInt(readable, "entryLength");
        return length - 4;
    }

    @Override
    public void writeRawEntry(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        byte[] rawEntry,
        int offset,
        int length,
        FormatTransformer appendKeyFormatTransormer,
        FormatTransformer appendValueFormatTransormer,
        IAppendOnly appendOnly) throws Exception {

        int entryLength = 4 + length + 4;
        UIO.writeInt(appendOnly, entryLength, "entryLength");
        appendOnly.append(rawEntry, offset, length);
        UIO.writeInt(appendOnly, entryLength, "entryLength");
    }

    @Override
    public byte[] key(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        byte[] rawEntry,
        int offset,
        int length) {

        int keyLength = UIO.bytesInt(rawEntry, offset);
        byte[] key = new byte[keyLength];
        System.arraycopy(rawEntry, 4, key, 0, keyLength);
        return key;
    }

    @Override
    public int compareKey(FormatTransformer readKeyFormatTransormer, FormatTransformer readValueFormatTransormer, byte[] rawEntry, int offset, byte[] compareKey,
        int compareOffset, int compareLength) {

        int keylength = UIO.bytesInt(rawEntry, offset);
        return IndexUtil.compare(rawEntry, offset + 4, keylength, compareKey, compareOffset, compareLength);
    }

    @Override
    public int compareKeyFromEntry(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        IReadable readable,
        byte[] compareKey,
        int compareOffset,
        int compareLength) throws Exception {

        readable.seek(readable.getFilePointer() + 4); // skip the entry length
        int keyLength = readable.readInt(); // keyLength
        return IndexUtil.compare(readable, keyLength, compareKey, compareOffset, compareLength);
    }

    @Override
    public int compare(byte[] o1, byte[] o2) {
        return UnsignedBytes.lexicographicalComparator().compare(o1, o2);
    }

    @Override
    public int compareKey(FormatTransformer aReadKeyFormatTransormer,
        FormatTransformer aReadValueFormatTransormer,
        byte[] aRawEntry,
        FormatTransformer bReadKeyFormatTransormer,
        FormatTransformer bReadValueFormatTransormer,
        byte[] bRawEntry) {

        if (aRawEntry == null && bRawEntry == null) {
            return 0;
        } else if (aRawEntry == null) {
            return -bRawEntry.length;
        } else if (bRawEntry == null) {
            return aRawEntry.length;
        } else {
            return compare(
                key(aReadKeyFormatTransormer, aReadValueFormatTransormer, aRawEntry, 0, aRawEntry.length),
                key(bReadKeyFormatTransormer, bReadValueFormatTransormer, bRawEntry, 0, bRawEntry.length)
            );
        }
    }
}
