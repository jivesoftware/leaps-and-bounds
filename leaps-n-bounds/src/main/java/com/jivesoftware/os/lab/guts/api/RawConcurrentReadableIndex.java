package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.guts.IndexRangeId;
import com.jivesoftware.os.lab.guts.TimestampAndVersion;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface RawConcurrentReadableIndex {

    String name();

    IndexRangeId id();

    byte[] minKey();

    byte[] maxKey();

    ReadIndex acquireReader() throws Exception;

    void destroy() throws Exception;

    boolean isEmpty() throws IOException;

    long count() throws IOException;

    void closeReadable() throws Exception;

    long sizeInBytes() throws IOException;

    long keysSizeInBytes() throws IOException;

    long valuesSizeInBytes() throws IOException;

    TimestampAndVersion maxTimestampAndVersion();

    boolean containsKeyInRange(byte[] from, byte[] to);

}
