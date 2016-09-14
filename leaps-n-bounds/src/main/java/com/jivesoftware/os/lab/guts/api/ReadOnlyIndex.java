package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.guts.IndexRangeId;
import com.jivesoftware.os.lab.guts.TimestampAndVersion;

/**
 *
 * @author jonathan.colt
 */
public interface ReadOnlyIndex {

    long keysSizeInBytes() throws Exception;

    long valuesSizeInBytes() throws Exception;

     String name();

    IndexRangeId id();

    byte[] minKey() throws Exception;

    byte[] maxKey() throws Exception;

    ReadIndex acquireReader() throws Exception;

    void destroy() throws Exception;

    boolean isEmpty() throws Exception;

    long count() throws Exception;

    void closeReadable() throws Exception;

    long sizeInBytes() throws Exception;

    TimestampAndVersion maxTimestampAndVersion();

    boolean containsKeyInRange(byte[] from, byte[] to) throws Exception;
}
