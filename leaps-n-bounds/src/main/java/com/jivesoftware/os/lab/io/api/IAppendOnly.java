package com.jivesoftware.os.lab.io.api;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface IAppendOnly extends ICloseable, IFilePointer {

    void append(byte b[], int _offset, int _len) throws IOException;

    void flush(boolean fsync) throws IOException;

}
