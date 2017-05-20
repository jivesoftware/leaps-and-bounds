package com.jivesoftware.os.lab.api.rawhide;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.PointerReadableByteBufferFile;
import com.jivesoftware.os.lab.io.api.IAppendOnly;

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

        BolBuffer key = rawEntry.sliceInto(0, keyLength, keyBuffer);
        BolBuffer payload = null;
        if (valueBuffer != null) {
            payload = rawEntry.sliceInto(keyLength, payloadLength, valueBuffer);
        }
        return stream.stream(index, key, 0, false, 0, payload);
    }

    @Override
    public BolBuffer toRawEntry(byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload, BolBuffer rawEntryBuffer) throws
        Exception {

        rawEntryBuffer.allocate(keyLength + payloadLength);
        System.arraycopy(key, 0, rawEntryBuffer.bytes, 0, keyLength);
        if (payloadLength > 0) {
            System.arraycopy(payload, 0, rawEntryBuffer.bytes, keyLength, payloadLength);
        }
        return rawEntryBuffer;
    }

    @Override
    public int rawEntryToBuffer(PointerReadableByteBufferFile readable, long offset, BolBuffer entryBuffer) throws Exception {
        readable.sliceIntoBuffer(offset, keyLength + payloadLength, entryBuffer);
        return keyLength + payloadLength;
    }

    @Override
    public void writeRawEntry(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntryBuffer,
        FormatTransformer writeKeyFormatTransformer,
        FormatTransformer writeValueFormatTransformer,
        IAppendOnly appendOnly) throws Exception {
        appendOnly.append(rawEntryBuffer);
    }

    @Override
    public BolBuffer key(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntry,
        BolBuffer keyBuffer) {
        rawEntry.sliceInto(0, keyLength, keyBuffer);
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
