package com.jivesoftware.os.lab;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class LABRawhide implements Rawhide {

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
        byte[] rawEntrys,
        int offset,
        int length) {

        int keyLength = UIO.bytesInt(rawEntrys);
        return UIO.bytesLong(rawEntrys, 4 + keyLength);
    }

    @Override
    public long version(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        byte[] rawEntry,
        int offset,
        int length) {

        int keyLength = UIO.bytesInt(rawEntry);
        return UIO.bytesLong(rawEntry, 4 + keyLength + 8 + 1);
    }

    @Override
    public boolean streamRawEntry(int index,
        FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        byte[] rawEntry,
        int offset,
        ValueStream valueStream,
        boolean hydrateValues) throws Exception {

        if (rawEntry == null) {
            return valueStream.stream(index, null, -1, false, -1, null);
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

        return valueStream.stream(index, readKeyFormatTransormer.transform(k), timestamp, tombstone, version, readValueFormatTransormer.transform(value));
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

        int keyLength = UIO.bytesInt(rawEntry, offset);
        byte[] key = new byte[keyLength];
        System.arraycopy(rawEntry, 4, key, 0, keyLength);
        return readKeyFormatTransormer.transform(key);
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
        int keyLength = readable.readInt(); // "keyLength"
        return IndexUtil.compare(readable, keyLength, compareKey, compareOffset, compareLength);
    }

    @Override
    public int compare(byte[] key1, byte[] key2) {
        return UnsignedBytes.lexicographicalComparator().compare(key1, key2);
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

    @Override
    public String toString() {
        return "LABRawhide{" + '}';
    }

}
