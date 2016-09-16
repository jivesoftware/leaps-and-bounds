package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface ReadIndex {

    void release();

    GetRaw get() throws Exception;

    Scanner rangeScan(byte[] from, byte[] to, BolBuffer entryBuffer) throws Exception;

    Scanner rowScan(BolBuffer entryBuffer) throws Exception;

    void close() throws Exception;

    long count() throws Exception;

    boolean isEmpty() throws Exception;

}
