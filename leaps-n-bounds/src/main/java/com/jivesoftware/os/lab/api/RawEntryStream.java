package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface RawEntryStream {

    boolean stream(byte[] rawEntry, int offset, int length) throws Exception;
}
