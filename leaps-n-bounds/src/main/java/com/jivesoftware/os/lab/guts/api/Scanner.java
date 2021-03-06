package com.jivesoftware.os.lab.guts.api;

/**
 *
 * @author jonathan.colt
 */
public interface Scanner {

    enum Next {
        eos, more, stopped
    }

    Next next(RawEntryStream stream) throws Exception;

    void close() throws Exception;
}
