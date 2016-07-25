package com.jivesoftware.os.lab.io.api;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface IAppendOnly extends ICloseable, IFilePointer {

    void appendByte(byte b) throws IOException;

    void appendShort(short s) throws IOException;

    void appendInt(int i) throws IOException;

    void appendLong(long l) throws IOException;

    void append(byte b[], int _offset, int _len) throws IOException;

    void flush(boolean fsync) throws IOException;

}
