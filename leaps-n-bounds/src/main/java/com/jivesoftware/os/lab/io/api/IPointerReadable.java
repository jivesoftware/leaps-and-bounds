package com.jivesoftware.os.lab.io.api;

import com.jivesoftware.os.lab.io.BolBuffer;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface IPointerReadable {

    long length();

    int read(long readPointer) throws IOException;

    int readInt(long readPointer) throws IOException;

    long readLong(long readPointer) throws IOException;

    int read(long readPointer, byte b[], int _offset, int _len) throws IOException;

    void close() throws IOException;

    BolBuffer sliceIntoBuffer(long offset, int i, BolBuffer entryBuffer) throws IOException;

}
