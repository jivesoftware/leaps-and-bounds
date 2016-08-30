package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.io.api.UIO;
import java.nio.ByteBuffer;

/**
 *
 * @author jonathan.colt
 */
public class BolBuffer {

    public byte[] bytes;
    public int offset;
    public int length;

    public BolBuffer() {
    }

    public BolBuffer(byte[] bytes) {
        this(bytes, 0, bytes == null ? -1 : bytes.length);
        if (bytes == null) {
            new RuntimeException().printStackTrace();
            System.exit(1);
        }
    }

    public BolBuffer(byte[] bytes, int offet, int length) {
        this.bytes = bytes;
        this.offset = offet;
        this.length = length;
    }

    public byte get(int offset) {
        return bytes[this.offset + offset];
    }

    public int getInt(int offset) {
        return UIO.bytesInt(bytes, this.offset + offset);
    }

    public long getLong(int offset) {
        return UIO.bytesLong(bytes, this.offset + offset);
    }

    public BolBuffer slice(int offset, int length) {
        return new BolBuffer(bytes, this.offset + offset, length);
    }

    public void allocate(int length) {
        if (bytes == null || bytes.length < length) {
            bytes = new byte[length];
        }
        this.length = length;
    }

    public byte[] copy() {
        byte[] copy = new byte[length];
        System.arraycopy(bytes, offset, copy, 0, length);
        return copy;
    }

    public void set(BolBuffer bolBuffer) {
        allocate(bolBuffer.length);
        offset = 0;
        length = bolBuffer.length;
        System.arraycopy(bolBuffer.bytes, bolBuffer.offset, bytes, 0, length);
    }

    public void set(byte[] raw) {
        bytes = raw;
        offset = 0;
        length = raw.length;
    }

    public ByteBuffer asByteBuffer() {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.position(offset);
        bb.limit(offset + length);
        return bb.slice();
    }
}
