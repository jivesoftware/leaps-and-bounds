package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface RawAppendableIndex {

    boolean append(AppendEntries entries, BolBuffer keyBuffer) throws Exception;

    void closeAppendable(boolean fsync) throws Exception;
}
