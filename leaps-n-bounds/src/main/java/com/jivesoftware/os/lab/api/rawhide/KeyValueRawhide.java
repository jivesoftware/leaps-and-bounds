package com.jivesoftware.os.lab.api.rawhide;

import com.jivesoftware.os.lab.LABUtils;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.PointerReadableByteBufferFile;
import com.jivesoftware.os.lab.io.api.IAppendOnly;

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
    public boolean streamRawEntry(int index,
        FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntry,
        BolBuffer keyBuffer,
        BolBuffer valueBuffer,
        ValueStream stream) throws Exception {

        if (rawEntry == null) {
            return stream.stream(index, null, -1, false, -1, null);
        }
        int keyLength = rawEntry.getInt(0);
        BolBuffer key = rawEntry.sliceInto(4, keyLength, keyBuffer);
        BolBuffer payload = null;
        if (valueBuffer != null) {
            int payloadLength = rawEntry.getInt(4 + keyLength);
            if (payloadLength >= 0) {
                payload = rawEntry.sliceInto(4 + keyLength + 4, payloadLength, valueBuffer);
            }
        }
        return stream.stream(index, key, 0, false, 0, payload);
    }

    @Override
    public BolBuffer toRawEntry(byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload, BolBuffer rawEntryBuffer) throws Exception {
        rawEntryBuffer.allocate(LABUtils.rawArrayLength(key) + LABUtils.rawArrayLength(payload));
        int o = 0;
        o = LABUtils.writeByteArray(key, rawEntryBuffer.bytes, o);
        LABUtils.writeByteArray(payload, rawEntryBuffer.bytes, o);
        return rawEntryBuffer;
    }

    @Override
    public int rawEntryToBuffer(PointerReadableByteBufferFile readable, long offset, BolBuffer entryBuffer) throws Exception {
        int length = readable.readInt(offset);
        readable.sliceIntoBuffer(offset + 4, length, entryBuffer);
        return 4 + length;
    }

    @Override
    public void writeRawEntry(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntryBuffer,
        FormatTransformer writeKeyFormatTransformer,
        FormatTransformer writeValueFormatTransformer,
        IAppendOnly appendOnly) throws Exception {
        appendOnly.appendInt(rawEntryBuffer.length);
        appendOnly.append(rawEntryBuffer);
    }

    @Override
    public BolBuffer key(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntry,
        BolBuffer keyBuffer) {
        rawEntry.sliceInto(4, rawEntry.getInt(0), keyBuffer);
        return keyBuffer;
    }

    @Override
    public boolean hasTimestampVersion() {
        return false;
    }

    @Override
    public long timestamp(FormatTransformer readKeyFormatTransformer, FormatTransformer readValueFormatTransformer, BolBuffer rawEntry) {
        return 0;
    }

    @Override
    public long version(FormatTransformer readKeyFormatTransformer, FormatTransformer readValueFormatTransformer, BolBuffer rawEntry) {
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
