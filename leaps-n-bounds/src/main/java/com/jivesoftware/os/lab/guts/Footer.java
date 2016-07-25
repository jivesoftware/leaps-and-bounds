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
    final long keyFormat;
    final long valueFormat;
    final TimestampAndVersion maxTimestampAndVersion;

    public Footer(int leapCount,
        long count,
        long keysSizeInBytes,
        long valuesSizeInBytes,
        byte[] minKey,
        byte[] maxKey,
        long keyFormat,
        long valueFormat,
        TimestampAndVersion maxTimestampAndVersion
    ) {

        this.leapCount = leapCount;
        this.count = count;
        this.keysSizeInBytes = keysSizeInBytes;
        this.valuesSizeInBytes = valuesSizeInBytes;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.keyFormat = keyFormat;
        this.valueFormat = valueFormat;
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
            + ", keyFormat=" + keyFormat
            + ", valueFormat=" + valueFormat
            + ", maxTimestampAndVersion=" + maxTimestampAndVersion
            + '}';
    }

    void write(IAppendOnly writeable) throws IOException {
        int entryLength = 4 + 4 + 8 + 8 + 8 + 4 + (minKey == null ? 0 : minKey.length) + 4 + (maxKey == null ? 0 : maxKey.length) + 8 + 8 + 8 + 8 + 4;
        UIO.writeInt(writeable, entryLength, "entryLength");
        UIO.writeInt(writeable, leapCount, "leapCount");
        UIO.writeLong(writeable, count, "count");
        UIO.writeLong(writeable, keysSizeInBytes, "keysSizeInBytes");
        UIO.writeLong(writeable, valuesSizeInBytes, "valuesSizeInBytes");
        UIO.writeByteArray(writeable, minKey, "minKey");
        UIO.writeByteArray(writeable, maxKey, "maxKey");
        UIO.writeLong(writeable, maxTimestampAndVersion.maxTimestamp, "maxTimestamp");
        UIO.writeLong(writeable, maxTimestampAndVersion.maxTimestampVersion, "maxTimestampVersion");
        UIO.writeLong(writeable, keyFormat, "keyFormat");
        UIO.writeLong(writeable, valueFormat, "valueFormat");
        UIO.writeInt(writeable, entryLength, "entryLength");
    }

    static Footer read(IReadable readable) throws IOException {
        int entryLength = UIO.readInt(readable, "entryLength");
        int read = 4;
        int leapCount = UIO.readInt(readable, "leapCount");
        read += 4;
        long count = UIO.readLong(readable, "count");
        read += 8;
        long keysSizeInBytes = UIO.readLong(readable, "keysSizeInBytes");
        read += 8;
        long valuesSizeInBytes = UIO.readLong(readable, "valuesSizeInBytes");
        read += 8;
        byte[] minKey = UIO.readByteArray(readable, "minKey");
        read += 4 + minKey.length;
        byte[] maxKey = UIO.readByteArray(readable, "maxKey");
        read += 4 + maxKey.length;
        long maxTimestamp = UIO.readLong(readable, "maxTimestamp");
        read += 8;
        long maxTimestampVersion = UIO.readLong(readable, "maxTimestampVersion");
        read += 8;

        long keyFormat = 0;
        long valueFormat = 0;
        if (entryLength > read) {
            keyFormat = UIO.readLong(readable, "keyFormat");
            valueFormat = UIO.readLong(readable, "valueFormat");
        }

        if (UIO.readInt(readable, "entryLength") != entryLength) {
            throw new RuntimeException("Encountered length corruption. ");
        }
        return new Footer(leapCount, count, keysSizeInBytes, valuesSizeInBytes, minKey, maxKey, keyFormat, valueFormat, new TimestampAndVersion(maxTimestamp,
            maxTimestampVersion));
    }

}
