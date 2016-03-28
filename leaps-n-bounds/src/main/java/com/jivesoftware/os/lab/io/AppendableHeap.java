/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.lab.io;

import com.google.common.math.IntMath;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import java.io.IOException;

/**
 *
 * All of the methods are intentionally left unsynchronized. Up to caller to do the right thing using the Object returned by lock()
 *
 */
public class AppendableHeap implements IAppendOnly {

    private byte[] bytes = new byte[0];
    private int fp = 0;
    private int maxLength = 0;

    public AppendableHeap(int size) {
        bytes = new byte[size];
        maxLength = 0;
    }

    public byte[] getBytes() {
        if (maxLength == bytes.length) {
            return bytes;
        } else {
            byte[] newSrc = new byte[maxLength];
            System.arraycopy(bytes, 0, newSrc, 0, maxLength);
            return newSrc;

        }
    }

    public byte[] leakBytes() {
        return bytes;
    }

    public void reset() {
        fp = 0;
        maxLength = 0;
    }

    @Override
    public void append(byte _b[], int _offset, int _len) throws IOException {
        if (fp + _len > bytes.length) {
            bytes = grow(bytes, Math.max(((fp + _len) - bytes.length), bytes.length));
        }
        System.arraycopy(_b, _offset, bytes, fp, _len);
        fp += _len;
        maxLength = Math.max(maxLength, fp);
    }

    @Override
    public long getFilePointer() throws IOException {
        return fp;
    }

    @Override
    public long length() throws IOException {
        return maxLength;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void flush(boolean fsync) throws IOException {
    }

    static final public byte[] grow(byte[] src, int amount) {
        if (src == null) {
            return new byte[amount];
        }
        byte[] newSrc = new byte[IntMath.checkedAdd(src.length, amount)];
        System.arraycopy(src, 0, newSrc, 0, src.length);
        return newSrc;
    }

}
