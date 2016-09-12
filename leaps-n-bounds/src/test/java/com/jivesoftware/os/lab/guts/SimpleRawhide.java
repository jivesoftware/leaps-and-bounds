package com.jivesoftware.os.lab.guts;

import com.google.common.primitives.Longs;
import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author jonathan.colt
 */
public class SimpleRawhide implements Rawhide {

    public static String toString(ByteBuffer rawEntry) {
        return "key:" + key(rawEntry) + " value:" + value(rawEntry);
    }

    @Override
    public BolBuffer merge(FormatTransformer currentReadKeyFormatTransormer,
        FormatTransformer currentReadValueFormatTransormer,
        BolBuffer currentRawEntry,
        FormatTransformer addingReadKeyFormatTransormer,
        FormatTransformer addingReadValueFormatTransormer,
        BolBuffer addingRawEntry,
        FormatTransformer mergedReadKeyFormatTransormer,
        FormatTransformer mergedReadValueFormatTransormer) {

        return value(currentRawEntry) > value(addingRawEntry) ? currentRawEntry : addingRawEntry;
    }

    @Override
    public int mergeCompare(FormatTransformer aReadKeyFormatTransormer, FormatTransformer aReadValueFormatTransormer, ByteBuffer aRawEntry,
        FormatTransformer bReadKeyFormatTransormer, FormatTransformer bReadValueFormatTransormer, ByteBuffer bRawEntry) {

        int c = compareKey(aReadKeyFormatTransormer, aReadValueFormatTransormer, aRawEntry, bReadKeyFormatTransormer, bReadValueFormatTransormer, bRawEntry);
        if (c != 0) {
            return c;
        }
        return -Longs.compare(value(aRawEntry), value(bRawEntry));
    }

    public static long key(ByteBuffer rawEntry) {
        if (rawEntry == null) {
            return 0;
        }
        rawEntry.clear();
        rawEntry.position(4);
        return rawEntry.getLong();
    }

    @Override
    public long timestamp(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        BolBuffer rawEntry) {

        return value(rawEntry);
    }

    @Override
    public long version(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        BolBuffer rawEntry) {

        return -1;
    }

    public static long value(byte[] rawEntry) {
        return UIO.bytesLong(rawEntry, 4 + 8);
    }

    public static long value(BolBuffer rawEntry) {
        return rawEntry.getLong(4 + 8);
    }

    public static long value(ByteBuffer rawEntry) {
        if (rawEntry == null) {
            return 0;
        }
        rawEntry.clear();
        rawEntry.position(4 + 8);
        return rawEntry.getLong();
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
        ByteBuffer rawEntry,
        ValueStream valueStream,
        boolean hydrateValues) throws Exception {

        if (rawEntry == null) {
            return valueStream.stream(index, null, -1, false, -1, null);
        }
        rawEntry.clear();
        int keyLength = rawEntry.getInt();
        rawEntry.limit(4 + keyLength);
        ByteBuffer key = rawEntry.slice();

        rawEntry.limit(4 + keyLength + 8 + 1 + 8);
        rawEntry.position(4 + keyLength);
        long timestamp = rawEntry.getLong();
        boolean tombstone = rawEntry.get() != 0;
        long version = rawEntry.getLong();

        ByteBuffer payload = null;
        if (hydrateValues) {
            rawEntry.limit(4 + keyLength + 8 + 1 + 8 + 4);
            rawEntry.position(4 + keyLength + 8 + 1 + 8);
            int payloadLength = rawEntry.getInt();
            rawEntry.limit(4 + keyLength + 8 + 1 + 8 + 4 + payloadLength);
            payload = rawEntry.slice();
        }

        return valueStream.stream(index, readKeyFormatTransormer.transform(key), timestamp, tombstone, version, readValueFormatTransormer.transform(payload));
    }

    @Override
    public BolBuffer toRawEntry(byte[] key, long timestamp, boolean tombstoned, long version, byte[] value, BolBuffer rawEntryBuffer) throws
        IOException {

        rawEntryBuffer.allocate(4 + key.length + 8 + 1 + 4 + value.length);
        int offset = 0;
        UIO.intBytes(key.length, rawEntryBuffer.bytes, offset);
        offset += 4;
        UIO.writeBytes(key, rawEntryBuffer.bytes, offset);
        offset += key.length;
        UIO.longBytes(timestamp, rawEntryBuffer.bytes, offset);
        offset += 8;
        rawEntryBuffer.bytes[offset] = tombstoned ? (byte) 1 : (byte) 0;
        offset++;
        UIO.longBytes(version, rawEntryBuffer.bytes, offset);
        offset += 8;
        UIO.intBytes(value.length, rawEntryBuffer.bytes, offset);
        offset += 4;
        UIO.writeBytes(value, rawEntryBuffer.bytes, offset);

        return rawEntryBuffer;
    }

    @Override
    public int rawEntryLength(IReadable readable) throws Exception {
        int length = UIO.readInt(readable, "entryLength");
        return length - 4;
    }

    @Override
    public void writeRawEntry(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        BolBuffer rawEntryBuffer,
        FormatTransformer appendKeyFormatTransormer,
        FormatTransformer appendValueFormatTransormer,
        IAppendOnly appendOnly) throws Exception {

        int entryLength = 4 + rawEntryBuffer.length + 4;
        UIO.writeInt(appendOnly, entryLength, "entryLength");
        appendOnly.append(rawEntryBuffer.bytes, rawEntryBuffer.offset, rawEntryBuffer.length);
        UIO.writeInt(appendOnly, entryLength, "entryLength");
    }

    @Override
    public BolBuffer key(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        BolBuffer rawEntry,
        BolBuffer keyBuffer) {
        int length = rawEntry.getInt(0);
        rawEntry.sliceInto(4, length, keyBuffer);
        return keyBuffer;
    }

    @Override
    public ByteBuffer key(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        ByteBuffer rawEntry
    ) {
        rawEntry.clear();
        int keyLength = rawEntry.getInt();
        rawEntry.limit(4 + keyLength);
        return rawEntry.slice();
    }

    @Override
    public int compareKey(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        ByteBuffer rawEntry,
        ByteBuffer compareKey
    ) {
        return IndexUtil.compare(key(readKeyFormatTransormer, readValueFormatTransormer, rawEntry), compareKey);
    }

    @Override
    public int compareKeyFromEntry(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        IReadable readable,
        ByteBuffer compareKey) throws Exception {
        readable.seek(readable.getFilePointer() + 4); // skip the entry length
        int keyLength = readable.readInt();
        return IndexUtil.compare(readable, keyLength, compareKey);
    }

    @Override
    public int compareKey(FormatTransformer aReadKeyFormatTransormer,
        FormatTransformer aReadValueFormatTransormer,
        ByteBuffer aRawEntry,
        FormatTransformer bReadKeyFormatTransormer,
        FormatTransformer bReadValueFormatTransormer,
        ByteBuffer bRawEntry) {

        if (aRawEntry == null && bRawEntry == null) {
            return 0;
        } else if (aRawEntry == null) {
            return -bRawEntry.capacity();
        } else if (bRawEntry == null) {
            return aRawEntry.capacity();
        } else {
            return IndexUtil.compare(
                key(aReadKeyFormatTransormer, aReadValueFormatTransormer, aRawEntry),
                key(bReadKeyFormatTransormer, bReadValueFormatTransormer, bRawEntry)
            );
        }
    }

}
