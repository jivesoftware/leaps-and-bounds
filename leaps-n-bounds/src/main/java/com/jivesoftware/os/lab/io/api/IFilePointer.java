package com.jivesoftware.os.lab.io.api;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface IFilePointer {

    /**
     *
     * @return
     */
    Object lock();

    /**
     *
     * @return @throws IOException
     */
    long length() throws IOException;

    /**
     *
     * @return @throws IOException
     */
    long getFilePointer() throws IOException;
}
