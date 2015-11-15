package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.IndexRangeId;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface RawConcurrentReadableIndex {

    IndexRangeId id();

    ReadIndex reader(int bufferSize) throws Exception;

    void destroy() throws Exception;

    boolean isEmpty() throws IOException;

    long count() throws IOException;

    void close() throws Exception;

}
