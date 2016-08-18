package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author jonathan.colt
 */
public class LABRawhide implements Rawhide {

    public static final String NAME = "labRawhide";
    public static final LABRawhide SINGLETON = new LABRawhide();

    private LABRawhide() {
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

        int currentKeyLength = UIO.bytesInt(currentRawEntry);
        int addingKeyLength = UIO.bytesInt(addingRawEntry);

        long currentsTimestamp = UIO.bytesLong(currentRawEntry, 4 + currentKeyLength);
        long currentsVersion = UIO.bytesLong(currentRawEntry, 4 + currentKeyLength + 8 + 1);

        long addingsTimestamp = UIO.bytesLong(addingRawEntry, 4 + addingKeyLength);
        long addingsVersion = UIO.bytesLong(addingRawEntry, 4 + addingKeyLength + 8 + 1);

        if ((currentsTimestamp > addingsTimestamp) || (currentsTimestamp == addingsTimestamp && currentsVersion > addingsVersion)) {
            return currentRawEntry;
        } else {
            return addingRawEntry;
        }
    }

    @Override
    public long timestamp(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        ByteBuffer rawEntrys) {

        rawEntrys.clear();
        int keyLength = rawEntrys.getInt();
        rawEntrys.position(4 + keyLength);
        return rawEntrys.getLong();
    }

    @Override
    public long version(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        ByteBuffer rawEntrys) {

        rawEntrys.clear();
        int keyLength = rawEntrys.getInt();
        rawEntrys.position(4 + keyLength + 8 + 1);
        return rawEntrys.getLong();
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
            if (payloadLength >= 0) {
                rawEntry.limit(4 + keyLength + 8 + 1 + 8 + 4 + payloadLength);
                payload = rawEntry.slice();
            }
        }
        return valueStream.stream(index, readKeyFormatTransormer.transform(key), timestamp, tombstone, version, readValueFormatTransormer.transform(payload));
    }

    @Override
    public byte[] toRawEntry(
        byte[] key,
        long timestamp,
        boolean tombstoned,
        long version,
        byte[] value) throws IOException {

        byte[] rawEntry = new byte[LABUtils.rawArrayLength(key) + 8 + 1 + 8 + LABUtils.rawArrayLength(value)];
        int o = 0;
        o = LABUtils.writeByteArray(key, rawEntry, o);
        UIO.longBytes(timestamp, rawEntry, o);
        o += 8;
        rawEntry[o] = tombstoned ? (byte) 1 : (byte) 0;
        o++;
        UIO.longBytes(version, rawEntry, o);
        o += 8;
        LABUtils.writeByteArray(value, rawEntry, o);
        return rawEntry;
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
        FormatTransformer writeKeyFormatTransormer,
        FormatTransformer writeValueFormatTransormer,
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
        return LABUtils.readByteArray(rawEntry, offset);
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
    public String toString() {
        return "LABRawhide{" + '}';
    }

}
