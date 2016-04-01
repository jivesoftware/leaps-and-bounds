package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface Keys {

    interface KeyStream {

        boolean key(int index, byte[] key, int offset, int length) throws Exception;
    }

    boolean keys(KeyStream keyStream) throws Exception;
}
