package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;

/**
 *
 * @author jonathan.colt
 */
public interface RawEntryMarshaller {

    byte[] merge(byte[] current, byte[] adding);

    boolean streamRawEntry(ValueStream stream, byte[] rawEntry, int offset) throws Exception;

    byte[] toRawEntry(byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) throws Exception;

    int entryLength(IReadable readable, byte[] lengthBuffer) throws Exception;

    void writeRawEntry(byte[] rawEntry, int offset, int length, IAppendOnly appendOnly, byte[] lengthBuffer) throws Exception;

    byte[] key(byte[] rawEntry, int offset, int length) throws Exception;

    long timestamp(byte[] rawEntry, int offset, int length);

    long version(byte[] rawEntry, int offset, int length);
}
