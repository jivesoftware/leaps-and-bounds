package com.jivesoftware.os.lab.guts.api;

/**
 *
 * @author jonathan.colt
 */
public interface ReadableIndex {

    byte[] minKey() throws Exception;

    byte[] maxKey() throws Exception;

    ReadIndex acquireReader() throws Exception;

    void destroy() throws Exception;

    boolean isEmpty() throws Exception;

    long count() throws Exception;

    void closeReadable() throws Exception;

    long sizeInBytes() throws Exception;

    boolean mightContain(long newerThanTimestamp, long newerThanTimestampVersion);

    boolean containsKeyInRange(byte[] from, byte[] to) throws Exception;
}
