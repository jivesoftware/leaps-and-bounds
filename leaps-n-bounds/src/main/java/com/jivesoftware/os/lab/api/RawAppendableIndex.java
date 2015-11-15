package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.IndexRangeId;

/**
 *
 * @author jonathan.colt
 */
public interface RawAppendableIndex {

    IndexRangeId id();

    boolean append(RawEntries entries) throws Exception;

    void close() throws Exception;
}
