package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;
import java.util.Comparator;

/**
 *
 * @author jonathan.colt
 */
public interface Rawhide extends Comparator<byte[]> {

    byte[] merge(RawEntryFormat currentFormat, byte[] currentRawEntry, RawEntryFormat addingFormat, byte[] addingRawEntry, RawEntryFormat mergedFromat);

    boolean streamRawEntry(ValueStream stream, int index, RawEntryFormat rawEntryFormat, byte[] rawEntry, int offset) throws Exception;

    byte[] toRawEntry(long keyFormat,
        byte[] key,
        long timestamp,
        boolean tombstoned,
        long version,
        long payloadFormat,
        byte[] payload,
        RawEntryFormat rawEntryFormat) throws Exception;

    int entryLength(RawEntryFormat readableFormat, IReadable readable, byte[] lengthBuffer) throws Exception;

    void writeRawEntry(RawEntryFormat rawEntryFormat,
        byte[] rawEntry,
        int offset,
        int length,
        RawEntryFormat appendFormat,
        IAppendOnly appendOnly,
        byte[] lengthBuffer) throws Exception;

    byte[] key(RawEntryFormat rawEntryFormat, byte[] rawEntry, int offset, int length) throws Exception;

    int keyLength(RawEntryFormat rawEntryFormat, byte[] rawEntry, int offset);

    int keyOffset(RawEntryFormat rawEntryFormat, byte[] rawEntry, int offset);

    int compareKey(RawEntryFormat rawEntryFormat, byte[] rawEntry, int offset, long compareKeyFormat, byte[] compareKey, int compareOffset, int compareLength);

    int compareKeyFromEntry(RawEntryFormat readableFormat,
        IReadable readable,
        long compareKeyFormat,
        byte[] compareKey,
        int compareOffset,
        int compareLength,
        byte[] intBuffer)
        throws Exception;

    long timestamp(RawEntryFormat rawEntryFormat, byte[] rawEntry, int offset, int length);

    long version(RawEntryFormat rawEntryFormat, byte[] rawEntry, int offset, int length);

    boolean mightContain(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion);

    boolean isNewerThan(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion);
}
