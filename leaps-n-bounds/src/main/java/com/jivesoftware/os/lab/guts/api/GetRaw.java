package com.jivesoftware.os.lab.guts.api;

/**
 *
 * @author jonathan.colt
 */
public interface GetRaw {

    boolean get(byte[] key, RawEntryStream stream) throws Exception;

    boolean result();
}
