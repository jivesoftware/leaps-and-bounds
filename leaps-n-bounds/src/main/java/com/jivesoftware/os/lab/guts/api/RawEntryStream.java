package com.jivesoftware.os.lab.guts.api;

import com.jivesoftware.os.lab.api.RawEntryFormat;

/**
 *
 * @author jonathan.colt
 */
public interface RawEntryStream {

    boolean stream(RawEntryFormat rawEntryFormat, byte[] rawEntry, int offset, int length) throws Exception;
}
