package com.jivesoftware.os.lab.guts.api;

/**
 *
 * @author jonathan.colt
 */
public interface ReadIndex {

    void release();

    GetRaw get() throws Exception;

    Scanner rangeScan(byte[] from, byte[] to) throws Exception;

    Scanner rowScan() throws Exception;

    void close() throws Exception;

    long count() throws Exception;

    boolean isEmpty() throws Exception;

}
