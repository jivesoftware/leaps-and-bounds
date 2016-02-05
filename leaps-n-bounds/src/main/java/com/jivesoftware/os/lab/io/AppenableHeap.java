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
public class AppenableHeap implements IAppendOnly {

    private byte[] bytes = new byte[0];
    private long fp = 0;
    private long maxLength = 0;

    public AppenableHeap() {
    }

    public AppenableHeap(int size) {
        bytes = new byte[size];
        maxLength = 0;
    }

    private AppenableHeap(byte[] _bytes, int _maxLength) {
        bytes = _bytes;
        maxLength = _maxLength;
    }

    public static AppenableHeap fromBytes(byte[] _bytes, int length) {
        return new AppenableHeap(_bytes, length);
    }

    public AppenableHeap createReadOnlyClone() {
        AppenableHeap heapFiler = new AppenableHeap();
        heapFiler.bytes = bytes;
        heapFiler.maxLength = maxLength;
        return heapFiler;
    }

    public byte[] getBytes() {
        if (maxLength == bytes.length) {
            return bytes;
        } else {
            return trim(bytes, (int) maxLength);
        }
    }

    public byte[] copyUsedBytes() {
        return trim(bytes, (int) maxLength);
    }

    public byte[] leakBytes() {
        return bytes;
    }

    public void reset() {
        fp = 0;
        maxLength = 0;
    }

    public void reset(int _maxLength) {
        fp = 0;
        maxLength = _maxLength;
    }

    @Override
    public void append(byte _b[], int _offset, int _len) throws IOException {
        if (_b == null) {
            return;
        }
        if (fp + _len > bytes.length) {
            bytes = grow(bytes, Math.max((int) ((fp + _len) - bytes.length), bytes.length));
        }
        System.arraycopy(_b, _offset, bytes, (int) fp, _len);
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

    private static byte[] trim(byte[] src, int count) {
        byte[] newSrc = new byte[count];
        System.arraycopy(src, 0, newSrc, 0, count);
        return newSrc;
    }

    static final public byte[] grow(byte[] src, int amount) {
        if (amount < 0) {
            throw new IllegalArgumentException("amount must be greater than zero");
        }
        if (src == null) {
            return new byte[amount];
        }
        byte[] newSrc = new byte[IntMath.checkedAdd(src.length, amount)];
        System.arraycopy(src, 0, newSrc, 0, src.length);
        return newSrc;
    }

}
