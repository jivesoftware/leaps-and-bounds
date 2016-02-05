package com.jivesoftware.os.lab.io.api;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface IFilePointer {

    long length() throws IOException;

    long getFilePointer() throws IOException;
}
