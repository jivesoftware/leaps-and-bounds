package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface NextRawEntry {

    boolean next(RawEntryStream stream) throws Exception;
}
