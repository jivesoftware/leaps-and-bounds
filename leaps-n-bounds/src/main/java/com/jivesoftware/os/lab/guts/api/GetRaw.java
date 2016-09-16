package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface GetRaw {

    boolean get(byte[] key, BolBuffer entryBuffer, RawEntryStream stream) throws Exception;

    boolean result();
}
