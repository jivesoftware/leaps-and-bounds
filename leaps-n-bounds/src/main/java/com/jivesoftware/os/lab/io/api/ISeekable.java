package com.jivesoftware.os.lab.io.api;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface ISeekable extends IFilePointer {

    /**
     *
     * @param position
     * @throws IOException
     */
    public void seek(long position) throws IOException;

}
