package com.jivesoftware.os.lab.guts.api;

/**
 *
 * @author jonathan.colt
 */
public interface RawAppendableIndex {

    
    boolean append(AppendEntries entries) throws Exception;

    void closeAppendable(boolean fsync) throws Exception;
}
