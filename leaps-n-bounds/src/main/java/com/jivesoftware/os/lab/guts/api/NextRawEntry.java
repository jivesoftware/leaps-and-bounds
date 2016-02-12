package com.jivesoftware.os.lab.guts.api;

/**
 *
 * @author jonathan.colt
 */
public interface NextRawEntry {

    static enum Next {
        eos, more, stopped;
    }

    Next next(RawEntryStream stream) throws Exception;
}
