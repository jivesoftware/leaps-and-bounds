package com.jivesoftware.os.lab.io.api;

import java.nio.ByteBuffer;

/**
 *
 */
public interface ByteBufferFactory {

    boolean exists();

    ByteBuffer allocate(int index, long size);

    ByteBuffer reallocate(int index, ByteBuffer oldBuffer, long newSize);

    long length();

    long nextLength(int index, long oldLength, long position);
}
