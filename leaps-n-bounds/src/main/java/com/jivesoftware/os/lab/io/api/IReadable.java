/*
 * IReadable.java.java
 *
 * Created on 03-12-2010 11:13:54 PM
 *
 * Copyright 2010 Jonathan Colt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.lab.io.api;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author Administrator
 */
public interface IReadable extends ICloseable, ISeekable {

    /**
     *
     * @return @throws IOException
     */
    int read() throws IOException;

     /**
     *
     * @return @throws IOException
     */
    short readShort() throws IOException;

     /**
     *
     * @return @throws IOException
     */
    int readInt() throws IOException;

     /**
     *
     * @return @throws IOException
     */
    long readLong() throws IOException;

    /**
     *
     * @param b
     * @return
     * @throws IOException
     */
    int read(byte b[]) throws IOException;

    /**
     *
     * @param b
     * @param _offset
     * @param _len
     * @return
     * @throws IOException
     */
    int read(byte b[], int _offset, int _len) throws IOException;

    /**
     *
     * @param length
     * @return
     */
    ByteBuffer slice(int length) throws IOException;

    /**
     *
     * @param length
     * @return
     * @throws IOException
     */
    boolean canSlice(int length) throws IOException;
}
