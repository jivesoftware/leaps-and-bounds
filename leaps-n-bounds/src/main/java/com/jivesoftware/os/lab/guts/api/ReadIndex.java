package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.guts.ActiveScan;
import com.jivesoftware.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface ReadIndex {

    void release();

    Scanner rangeScan(ActiveScan activeScan, byte[] from, byte[] to, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception;

    Scanner rowScan(ActiveScan activeScan, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception;

    Scanner pointScan(ActiveScan activeScan, byte[] key, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception;

    long count() throws Exception;

}
