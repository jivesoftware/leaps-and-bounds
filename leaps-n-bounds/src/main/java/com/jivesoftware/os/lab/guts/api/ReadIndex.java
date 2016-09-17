package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface ReadIndex {

    void release();

    GetRaw get() throws Exception;

    Scanner rangeScan(byte[] from, byte[] to, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception;

    Scanner rowScan(BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception;

    long count() throws Exception;

}
