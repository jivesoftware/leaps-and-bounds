package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface ValueStream {

    boolean stream(int index, byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) throws Exception;
}
