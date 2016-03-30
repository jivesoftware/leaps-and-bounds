package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.io.api.UIO;

/**
 *
 * @author jonathan.colt
 */
public class LABUtils {

    public static int compare(long timestamp, long timestampVersion, long otherTimestamp, long otherTimestampVersion) {
        int c = Long.compare(timestamp, otherTimestamp);
        if (c != 0) {
            return c;
        }
        return Long.compare(timestampVersion, otherTimestampVersion);
    }

    public static int rawArrayLength(byte[] bytes) {
        return 4 + ((bytes == null) ? 0 : bytes.length);
    }

    public static byte[] readByteArray(byte[] source, int offset) {
        int o = offset;
        byte[] bytes = null;
        int bytesLength = UIO.bytesInt(source, o);
        o += 4;
        if (bytesLength > -1) {
            bytes = new byte[bytesLength];
            UIO.readBytes(source, o, bytes);
            o += bytes.length;
        }
        return bytes;
    }

    public static int writeByteArray(byte[] bytes, byte[] destination, int offset) {
        int o = offset;
        if (bytes != null) {
            UIO.intBytes(bytes.length, destination, o);
            o += 4;
            UIO.writeBytes(bytes, destination, o);
            o += bytes.length;
        } else {
            UIO.intBytes(-1, destination, o);
            o += 4;
        }
        return o;
    }
}
