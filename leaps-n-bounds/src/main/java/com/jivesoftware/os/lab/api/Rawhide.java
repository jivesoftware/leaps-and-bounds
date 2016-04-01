package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;

/**
 *
 * @author jonathan.colt
 */
public interface Rawhide {

    byte[] merge(byte[] current, byte[] adding);

    boolean streamRawEntry(ValueStream stream, int index, byte[] rawEntry, int offset) throws Exception;

    byte[] toRawEntry(byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) throws Exception;

    int entryLength(IReadable readable, byte[] lengthBuffer) throws Exception;

    void writeRawEntry(byte[] rawEntry, int offset, int length, IAppendOnly appendOnly, byte[] lengthBuffer) throws Exception;

    byte[] key(byte[] rawEntry, int offset, int length) throws Exception;

    int keyLength(byte[] rawEntry, int offset);

    int keyOffset(byte[] rawEntry, int offset);

    int compareKey(byte[] rawEntry, int offset, byte[] compareKey, int compareOffset, int compareLength);

    int compareKeyFromEntry(IReadable readable, byte[] compareKey, int compareOffset, int compareLength, byte[] intBuffer) throws Exception;

    long timestamp(byte[] rawEntry, int offset, int length);

    long version(byte[] rawEntry, int offset, int length);

    boolean mightContain(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion);

    boolean isNewerThan(long timestamp, long timestampVersion, long newerThanTimestamp, long newerThanTimestampVersion);
}
