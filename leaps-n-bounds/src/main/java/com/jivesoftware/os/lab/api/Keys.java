package com.jivesoftware.os.lab.api;

/**
 *
 * @author jonathan.colt
 */
public interface Keys {

    interface KeyStream {

        boolean key(byte[] key, int offset, int length);
    }

    boolean keys(KeyStream keyStream);
}
