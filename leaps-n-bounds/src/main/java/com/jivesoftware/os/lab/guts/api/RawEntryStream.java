package com.jivesoftware.os.lab.guts.api;

/**
 *
 * @author jonathan.colt
 */
public interface RawEntryStream {

    boolean stream(byte[] rawEntry, int offset, int length) throws Exception;
}
