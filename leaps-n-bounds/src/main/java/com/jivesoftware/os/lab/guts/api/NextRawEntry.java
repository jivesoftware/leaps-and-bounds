package com.jivesoftware.os.lab.guts.api;

/**
 *
 * @author jonathan.colt
 */
public interface NextRawEntry {

    boolean next(RawEntryStream stream) throws Exception;
}
