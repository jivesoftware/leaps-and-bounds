package com.jivesoftware.os.lab.api.rawhide;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IPointerReadable;
import java.util.Comparator;

/**
 *
 * @author jonathan.colt
 */
public interface Rawhide {

    BolBuffer key(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        BolBuffer rawEntry,
        BolBuffer keyBuffer) throws Exception;

    long timestamp(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        BolBuffer rawEntry);

    long version(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        BolBuffer rawEntry);

    boolean streamRawEntry(int index,
        FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
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

    int rawEntryToBuffer(IPointerReadable readable, long offset, BolBuffer entryBuffer) throws Exception;

    void writeRawEntry(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        BolBuffer rawEntryBuffer,
        FormatTransformer writeKeyFormatTransormer,
        FormatTransformer writeValueFormatTransormer,
        IAppendOnly appendOnly) throws Exception;

    // Default impls from here on out
    default BolBuffer merge(FormatTransformer currentReadKeyFormatTransormer,
        FormatTransformer currentReadValueFormatTransormer,
        BolBuffer currentRawEntry,
        FormatTransformer addingReadKeyFormatTransormer,
        FormatTransformer addingReadValueFormatTransormer,
        BolBuffer addingRawEntry,
        FormatTransformer mergedReadKeyFormatTransormer,
        FormatTransformer mergedReadValueFormatTransormer) {
        return addingRawEntry;
    }

    default int mergeCompare(FormatTransformer aReadKeyFormatTransormer,
        FormatTransformer aReadValueFormatTransormer,
        BolBuffer aRawEntry,
        BolBuffer aKeyBuffer,
        FormatTransformer bReadKeyFormatTransormer,
        FormatTransformer bReadValueFormatTransormer,
        BolBuffer bRawEntry,
        BolBuffer bKeyBuffer) throws Exception {

        return compareKey(aReadKeyFormatTransormer, aReadValueFormatTransormer, aRawEntry, aKeyBuffer,
            bReadKeyFormatTransormer, bReadValueFormatTransormer, bRawEntry, bKeyBuffer);
    }

    default int compareKey(FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        BolBuffer rawEntry,
        BolBuffer keyBuffer,
        BolBuffer compareKey
    ) throws Exception {
        return IndexUtil.compare(key(readKeyFormatTransormer, readValueFormatTransormer, rawEntry, keyBuffer), compareKey);
    }

    default int compareKey(FormatTransformer aReadKeyFormatTransormer,
        FormatTransformer aReadValueFormatTransormer,
        BolBuffer aRawEntry,
        BolBuffer aKeyBuffer,
        FormatTransformer bReadKeyFormatTransormer,
        FormatTransformer bReadValueFormatTransormer,
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
                key(aReadKeyFormatTransormer, aReadValueFormatTransormer, aRawEntry, aKeyBuffer),
                key(bReadKeyFormatTransormer, bReadValueFormatTransormer, bRawEntry, bKeyBuffer)
            );
        }
    }


    final Comparator<BolBuffer> bolBufferKeyComparator = IndexUtil::compare;

    default Comparator<BolBuffer> getBolBufferKeyComparator() {
        return bolBufferKeyComparator;
    }

    final Comparator<byte[]> keyComparator = (byte[] o1, byte[] o2) -> IndexUtil.compare(o1, 0, o1.length, o2, 0, o2.length);

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

    default int compareAB(BolBuffer left, byte[] right, int rightOffset, int rightLength) {
        return IndexUtil.compare(left, right, rightOffset, rightLength);
    }

}
