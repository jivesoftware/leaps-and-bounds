package com.jivesoftware.os.lab.guts.api;

/**
 *
 * @author jonathan.colt
 */
public interface ReadIndex {

    boolean acquire() throws InterruptedException;

    void release();

    GetRaw get() throws Exception;

    NextRawEntry rangeScan(byte[] from, byte[] to) throws Exception;

    NextRawEntry rowScan() throws Exception;

    void close() throws Exception;

    long count() throws Exception;

    boolean isEmpty() throws Exception;

}
