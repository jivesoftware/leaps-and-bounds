package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.LABUtils;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import java.nio.ByteBuffer;

/**
 *
 * @author jonathan.colt
 */
public class KeyValueRawhide implements Rawhide {

    public static final String NAME = "keyValueRawhide";

    public static final KeyValueRawhide SINGLETON = new KeyValueRawhide();

    private KeyValueRawhide() {
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
        int keyLength = rawEntry.getInt();
        rawEntry.limit(4 + keyLength);
        ByteBuffer key = rawEntry.slice();

        ByteBuffer payload = null;
        if (hydrateValues) {
            rawEntry.position(4 + keyLength);
            rawEntry.limit(4 + keyLength + 4);
            int payloadLength = rawEntry.getInt();
            if (payloadLength >= 0) {
                rawEntry.limit(4 + keyLength + 4 + payloadLength);
                payload = rawEntry.slice();
            }
        }
        return stream.stream(index, key, 0, false, 0, payload);
    }

    @Override
    public BolBuffer toRawEntry(byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload, BolBuffer rawEntryBuffer) throws Exception {
        rawEntryBuffer.allocate(LABUtils.rawArrayLength(key) + LABUtils.rawArrayLength(payload));
        int o = 0;
        o += LABUtils.writeByteArray(key, rawEntryBuffer.bytes, o);
        LABUtils.writeByteArray(payload, rawEntryBuffer.bytes, o);
        return rawEntryBuffer;
    }

    @Override
    public int rawEntryLength(IReadable readable) throws Exception {
        return readable.readInt();
    }

    @Override
    public void writeRawEntry(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        BolBuffer rawEntryBuffer,
        FormatTransformer writeKeyFormatTransormer,
        FormatTransformer writeValueFormatTransormer,
        IAppendOnly appendOnly) throws Exception {
        UIO.writeByteArray(appendOnly, rawEntryBuffer.bytes, rawEntryBuffer.offset, rawEntryBuffer.length, "entry");
    }

    @Override
    public BolBuffer key(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        BolBuffer rawEntry) {
        int keyLength = rawEntry.getInt(0);
        return rawEntry.slice(4, keyLength);
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

    @Override
    public long timestamp(FormatTransformer readKeyFormatTransormer, FormatTransformer readValueFormatTransormer, BolBuffer rawEntry) {
        return 0;
    }

    @Override
    public long version(FormatTransformer readKeyFormatTransormer, FormatTransformer readValueFormatTransormer, BolBuffer rawEntry) {
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
