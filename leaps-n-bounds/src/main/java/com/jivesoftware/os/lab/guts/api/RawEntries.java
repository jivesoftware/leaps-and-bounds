package com.jivesoftware.os.lab.guts.api;

/**
 *
 * @author jonathan.colt
 */
public interface RawEntries {

    boolean consume(RawEntryStream stream) throws Exception;
}
