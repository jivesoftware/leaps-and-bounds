package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.guts.IndexRangeId;

/**
 *
 * @author jonathan.colt
 */
public interface RawAppendableIndex {

    IndexRangeId id();

    boolean append(RawEntries entries) throws Exception;

    void closeAppendable(boolean fsync) throws Exception;
}
