package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.io.api.IReadable;

/**
 *
 * @author jonathan.colt
 */
public class ReadBuffer {

    private byte[] buffer;
    private int length;

    public byte[] buffer(IReadable readable, int length) throws Exception {
        if (buffer == null | buffer.length < length) {
            buffer = new byte[length];
        }
        readable.read(buffer, 0, length);
        return buffer;
    }

    public int length() {
        return length;
    }
}
