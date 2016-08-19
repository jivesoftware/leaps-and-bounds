package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import java.nio.ByteBuffer;

/**
 *
 * @author jonathan.colt
 */
public class FixedWidthRawhide implements Rawhide {

    private final int keyLength;
    private final int payloadLength;

    public FixedWidthRawhide(int keyLength, int payloadLength) {
        this.keyLength = keyLength;
        this.payloadLength = payloadLength;
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
        return addingRawEntry;
    }

    @Override
    public boolean streamRawEntry(int index,
        FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        ByteBuffer rawEntry,
        ValueStream stream,
        boolean hydrateValues) throws Exception {
        if (rawEntry == null) {
            return stream.stream(index, null, -1, false, -1, null);
        }

        rawEntry.clear();
        rawEntry.limit(keyLength);
        ByteBuffer key = rawEntry.slice();
        ByteBuffer payload = null;
        if (hydrateValues) {
            rawEntry.position(keyLength);
            rawEntry.limit(keyLength + payloadLength);
            payload = rawEntry.slice();
        }
        return stream.stream(index, key, 0, false, 0, payload);
    }

    @Override
    public byte[] toRawEntry(byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) throws Exception {
        return payload != null ? UIO.add(key, payload) : key;
    }

    @Override
    public int rawEntryLength(IReadable readable) throws Exception {
        return keyLength + payloadLength;
    }

    @Override
    public void writeRawEntry(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        byte[] rawEntry,
        int offset,
        int length,
        FormatTransformer writeKeyFormatTransormer,
        FormatTransformer writeValueFormatTransormer,
        IAppendOnly appendOnly) throws Exception {
        appendOnly.append(rawEntry, offset, length);
    }

    @Override
    public byte[] key(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        byte[] rawEntry,
        int offset,
        int length) {
        byte[] key = new byte[keyLength];
        UIO.readBytes(rawEntry, 0, key);
        return key;
    }

    @Override
    public ByteBuffer key(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        ByteBuffer rawEntry) {
        rawEntry.clear();
        rawEntry.limit(keyLength);
        return rawEntry.slice();
    }

    @Override
    public int compareKey(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        ByteBuffer rawEntry,
        ByteBuffer compareKey) {
        rawEntry.clear();
        rawEntry.limit(keyLength);
        return IndexUtil.compare(rawEntry.slice(), compareKey);
    }

    @Override
    public int compareKeyFromEntry(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        IReadable readable,
        ByteBuffer compareKey) throws Exception {

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

    @Override
    public long timestamp(FormatTransformer readKeyFormatTransormer, FormatTransformer readValueFormatTransormer, ByteBuffer rawEntry) {
        return 0;
    }

    @Override
    public long version(FormatTransformer readKeyFormatTransormer, FormatTransformer readValueFormatTransormer, ByteBuffer rawEntry) {
        return 0;
    }

    @Override
    public boolean isNewerThan(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion) {
        return true;
    }

    @Override
    public boolean mightContain(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion) {
        return (timestamp != -1 && timestampVersion != -1);
    }
}
