package com.jivesoftware.os.lab.api.rawhide;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.PointerReadableByteBufferFile;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import java.util.Comparator;

/**
 * @author jonathan.colt
 */
public interface Rawhide {

    BolBuffer key(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntry,
        BolBuffer keyBuffer) throws Exception;

    boolean hasTimestampVersion();

    long timestamp(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntry);

    long version(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntry);

    boolean streamRawEntry(int index,
        FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntry,
        BolBuffer keyBuffer,
        BolBuffer valueBuffer,
        ValueStream stream) throws Exception;

    BolBuffer toRawEntry(byte[] key,
        long timestamp,
        boolean tombstoned,
        long version,
        byte[] value,
        BolBuffer rawEntryBuffer) throws Exception;

    int rawEntryToBuffer(PointerReadableByteBufferFile readable, long offset, BolBuffer entryBuffer) throws Exception;

    void writeRawEntry(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntryBuffer,
        FormatTransformer writeKeyFormatTransformer,
        FormatTransformer writeValueFormatTransformer,
        IAppendOnly appendOnly) throws Exception;

    // Default impls from here on out
    default BolBuffer merge(FormatTransformer currentReadKeyFormatTransformer,
        FormatTransformer currentReadValueFormatTransformer,
        BolBuffer currentRawEntry,
        FormatTransformer addingReadKeyFormatTransformer,
        FormatTransformer addingReadValueFormatTransformer,
        BolBuffer addingRawEntry,
        FormatTransformer mergedReadKeyFormatTransformer,
        FormatTransformer mergedReadValueFormatTransformer) {
        return addingRawEntry;
    }

    default int mergeCompare(FormatTransformer aReadKeyFormatTransformer,
        FormatTransformer aReadValueFormatTransformer,
        BolBuffer aRawEntry,
        BolBuffer aKeyBuffer,
        FormatTransformer bReadKeyFormatTransformer,
        FormatTransformer bReadValueFormatTransformer,
        BolBuffer bRawEntry,
        BolBuffer bKeyBuffer) throws Exception {

        return compareKey(aReadKeyFormatTransformer, aReadValueFormatTransformer, aRawEntry, aKeyBuffer,
            bReadKeyFormatTransformer, bReadValueFormatTransformer, bRawEntry, bKeyBuffer);
    }

    default int compareKey(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        BolBuffer rawEntry,
        BolBuffer keyBuffer,
        BolBuffer compareKey
    ) throws Exception {
        return IndexUtil.compare(key(readKeyFormatTransformer, readValueFormatTransformer, rawEntry, keyBuffer), compareKey);
    }

    default int compareKey(FormatTransformer aReadKeyFormatTransformer,
        FormatTransformer aReadValueFormatTransformer,
        BolBuffer aRawEntry,
        BolBuffer aKeyBuffer,
        FormatTransformer bReadKeyFormatTransformer,
        FormatTransformer bReadValueFormatTransformer,
        BolBuffer bRawEntry,
        BolBuffer bKeyBuffer) throws Exception {

        if (aRawEntry == null && bRawEntry == null) {
            return 0;
        } else if (aRawEntry == null) {
            return -bRawEntry.length;
        } else if (bRawEntry == null) {
            return aRawEntry.length;
        } else {
            return IndexUtil.compare(
                key(aReadKeyFormatTransformer, aReadValueFormatTransformer, aRawEntry, aKeyBuffer),
                key(bReadKeyFormatTransformer, bReadValueFormatTransformer, bRawEntry, bKeyBuffer)
            );
        }
    }

    Comparator<BolBuffer> bolBufferKeyComparator = IndexUtil::compare;

    default Comparator<BolBuffer> getBolBufferKeyComparator() {
        return bolBufferKeyComparator;
    }

    Comparator<byte[]> keyComparator = (byte[] o1, byte[] o2) -> IndexUtil.compare(o1, 0, o1.length, o2, 0, o2.length);

    default Comparator<byte[]> getKeyComparator() {
        return keyComparator;
    }

    default boolean mightContain(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion) {
        return Rawhide.this.compare(timestamp, timestampVersion, newerThanTimestamp, newerThanTimestampVersion) >= 0;
    }

    default boolean isNewerThan(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion) {
        return Rawhide.this.compare(timestamp, timestampVersion, newerThanTimestamp, newerThanTimestampVersion) > 0;
    }

    default int compare(long timestamp, long timestampVersion, long otherTimestamp, long otherTimestampVersion) {
        int c = Long.compare(timestamp, otherTimestamp);
        if (c != 0) {
            return c;
        }
        return Long.compare(timestampVersion, otherTimestampVersion);
    }

    default int compareBB(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        return IndexUtil.compare(left, leftOffset, leftLength, right, rightOffset, rightLength);
    }

    default int compare(BolBuffer aKey, BolBuffer bKey) {
        return IndexUtil.compare(aKey, bKey);
    }

}
