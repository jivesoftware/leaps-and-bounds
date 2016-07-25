/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.lab.io;

import com.jivesoftware.os.lab.io.api.IFiler;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 *
 * @author jonathan.colt
 */
public class ByteBufferBackedFiler implements IFiler {

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

    @Override
    public long skip(long position) throws IOException {
        int p = buffer.position();
        p += position;
        buffer.position(p);
        return p;
    }

    public boolean hasRemaining(int len) {
        return buffer.remaining() >= len;
    }

    @Override
    public long length() throws IOException {
        return buffer.capacity();
    }

    @Override
    public void setLength(long len) throws IOException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public long getFilePointer() throws IOException {
        return buffer.position();
    }

    @Override
    public void eof() throws IOException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public void flush(boolean fsync) throws IOException {
        if (fsync && buffer instanceof MappedByteBuffer) {
            ((MappedByteBuffer) buffer).force();
        }
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
    public short readShort() throws IOException {
        return buffer.getShort();
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
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
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

    @Override
    public void writeByte(byte b) throws IOException {
        buffer.put(b);
    }

    @Override
    public void writeShort(short s) throws IOException {
        buffer.putShort(s);
    }

    @Override
    public void writeInt(int i) throws IOException {
        buffer.putInt(i);
    }

    @Override
    public void writeLong(long l) throws IOException {
        buffer.putLong(l);
    }

    @Override
    public void write(byte[] b, int _offset, int _len) throws IOException {
        buffer.put(b, _offset, _len);
    }

}
