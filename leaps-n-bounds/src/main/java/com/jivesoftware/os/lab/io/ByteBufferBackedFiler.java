/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.lab.io;

import com.jivesoftware.os.lab.io.api.IReadable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author jonathan.colt
 */
public class ByteBufferBackedFiler implements IReadable {

    final ByteBuffer buffer;

    public ByteBufferBackedFiler(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public ByteBufferBackedFiler duplicate() {
        return new ByteBufferBackedFiler(buffer.duplicate());
    }

    @Override
    public void seek(long position) throws IOException {
        buffer.position((int) position); // what a pain! limited to an int!
    }

    public boolean hasRemaining(int len) {
        return buffer.remaining() >= len;
    }

    @Override
    public long length() throws IOException {
        return buffer.capacity();
    }

    @Override
    public long getFilePointer() throws IOException {
        return buffer.position();
    }

    @Override
    public int read() throws IOException {
        int remaining = buffer.remaining();
        if (remaining == 0) {
            return -1;
        }
        byte b = buffer.get();
        return b & 0xFF;
    }

    @Override
    public int readInt() throws IOException {
        return buffer.getInt();
    }

    @Override
    public long readLong() throws IOException {
        return buffer.getLong();
    }

    @Override
    public int read(byte[] b, int _offset, int _len) throws IOException {
        int remaining = buffer.remaining();
        if (remaining == 0) {
            return -1;
        }
        int count = Math.min(_len, remaining);
        buffer.get(b, _offset, count);
        return count;
    }

    @Override
    public boolean canSlice(int length) throws IOException {
        long remaining = length() - getFilePointer();
        return remaining >= length;
    }

    @Override
    public ByteBuffer slice(int length) throws IOException {
        ByteBuffer duplicated = buffer.duplicate();
        int sliceToLimit = buffer.position() + length;
        duplicated.limit(sliceToLimit);
        buffer.position(sliceToLimit);
        return duplicated.slice();
    }

    @Override
    public void close() throws IOException {

    }
}
