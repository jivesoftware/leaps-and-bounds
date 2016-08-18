package com.jivesoftware.os.lab.api;

import java.nio.ByteBuffer;

/**
 *
 * @author jonathan.colt
 */
public interface ValueStream {

    boolean stream(int index, ByteBuffer key, long timestamp, boolean tombstoned, long version, ByteBuffer payload) throws Exception;
}
