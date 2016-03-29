package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.IOException;
import java.util.Arrays;

/**
 *
 * @author jonathan.colt
 */
public class Footer {

    final int leapCount;
    final long count;
    final long keysSizeInBytes;
    final long valuesSizeInBytes;
    final byte[] minKey;
    final byte[] maxKey;
    final TimestampAndVersion maxTimestampAndVersion;

    public Footer(int leapCount,
        long count,
        long keysSizeInBytes,
        long valuesSizeInBytes,
        byte[] minKey,
        byte[] maxKey,
        TimestampAndVersion maxTimestampAndVersion
    ) {

        this.leapCount = leapCount;
        this.count = count;
        this.keysSizeInBytes = keysSizeInBytes;
        this.valuesSizeInBytes = valuesSizeInBytes;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.maxTimestampAndVersion = maxTimestampAndVersion;
    }

    @Override
    public String toString() {
        return "Footer{"
            + "leapCount=" + leapCount
            + ", count=" + count
            + ", keysSizeInBytes=" + keysSizeInBytes
            + ", valuesSizeInBytes=" + valuesSizeInBytes
            + ", minKey=" + Arrays.toString(minKey)
            + ", maxKey=" + Arrays.toString(maxKey)
            + ", maxTimestampAndVersion=" + maxTimestampAndVersion
            + '}';
    }

    void write(IAppendOnly writeable, byte[] lengthBuffer) throws IOException {
        int entryLength = 4 + 4 + 8 + 8 + 8 + 4 + (minKey == null ? 0 : minKey.length) + 4 + (maxKey == null ? 0 : maxKey.length) + 8 + 8 + 4;
        UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
        UIO.writeInt(writeable, leapCount, "leapCount", lengthBuffer);
        UIO.writeLong(writeable, count, "count");
        UIO.writeLong(writeable, keysSizeInBytes, "keysSizeInBytes");
        UIO.writeLong(writeable, valuesSizeInBytes, "valuesSizeInBytes");
        UIO.writeByteArray(writeable, minKey, "minKey", lengthBuffer);
        UIO.writeByteArray(writeable, maxKey, "maxKey", lengthBuffer);
        UIO.writeLong(writeable, maxTimestampAndVersion.maxTimestamp, "maxTimestamp");
        UIO.writeLong(writeable, maxTimestampAndVersion.maxTimestampVersion, "maxTimestampVersion");
        UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
    }

    static Footer read(IReadable readable, byte[] lengthBuffer) throws IOException {
        int entryLength = UIO.readInt(readable, "entryLength", lengthBuffer);
        int leapCount = UIO.readInt(readable, "leapCount", lengthBuffer);
        long count = UIO.readLong(readable, "count", lengthBuffer);
        long keysSizeInBytes = UIO.readLong(readable, "keysSizeInBytes", lengthBuffer);
        long valuesSizeInBytes = UIO.readLong(readable, "valuesSizeInBytes", lengthBuffer);
        byte[] minKey = UIO.readByteArray(readable, "minKey", lengthBuffer);
        byte[] maxKey = UIO.readByteArray(readable, "minKey", lengthBuffer);
        long maxTimestamp = UIO.readLong(readable, "maxTimestamp", lengthBuffer);
        long maxTimestampVersion = UIO.readLong(readable, "maxTimestampVersion", lengthBuffer);

        if (UIO.readInt(readable, "entryLength", lengthBuffer) != entryLength) {
            throw new RuntimeException("Encountered length corruption. ");
        }
        return new Footer(leapCount, count, keysSizeInBytes, valuesSizeInBytes, minKey, maxKey, new TimestampAndVersion(maxTimestamp, maxTimestampVersion));
    }

}
