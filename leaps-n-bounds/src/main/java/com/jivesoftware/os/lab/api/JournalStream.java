package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.io.BolBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface JournalStream {

    boolean stream(byte[] valueIndexId, byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) throws Exception;
}
