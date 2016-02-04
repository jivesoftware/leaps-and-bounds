package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.guts.IndexRangeId;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface RawConcurrentReadableIndex {

    String name();
    
    IndexRangeId id();

    ReadIndex reader() throws Exception;

    void destroy() throws Exception;

    boolean isEmpty() throws IOException;

    long count() throws IOException;

    void close() throws Exception;

}