package com.jivesoftware.os.lab.io;

import com.jivesoftware.os.lab.io.api.IPointerReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 *
 */
public class PointerReadableByteBufferFile implements IPointerReadable {

    public static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static final long MAX_BUFFER_SEGMENT_SIZE = UIO.chunkLength(30);
    public static final long MAX_POSITION = MAX_BUFFER_SEGMENT_SIZE * 100;

    private final long maxBufferSegmentSize;
    private final File file;
    private final boolean writeable;
    private final long length;

    private final ByteBuffer[] bbs;

    private final int fShift;
    private final long fseekMask;

    public PointerReadableByteBufferFile(long maxBufferSegmentSize,
        File file, boolean writeable) throws IOException {

        this.maxBufferSegmentSize = Math.min(UIO.chunkLength(UIO.chunkPower(maxBufferSegmentSize, 0)), MAX_BUFFER_SEGMENT_SIZE);

        this.file = file;
        this.writeable = writeable;

        // test power of 2
        if ((this.maxBufferSegmentSize & (this.maxBufferSegmentSize - 1)) == 0) {
            this.fShift = Long.numberOfTrailingZeros(this.maxBufferSegmentSize);
            this.fseekMask = this.maxBufferSegmentSize - 1;
        } else {
            throw new IllegalArgumentException("It's hard to ensure powers of 2");
        }
        this.length = file.length();
        long position = this.length;
        int filerIndex = (int) (position >> fShift);
        long filerSeek = position & fseekMask;

        int newLength = filerIndex + 1;
        ByteBuffer[] newFilers = new ByteBuffer[newLength];
        for (int n = 0; n < newLength; n++) {
            if (n < newLength - 1) {
                newFilers[n] = allocate(n, maxBufferSegmentSize);
            } else {
                newFilers[n] = allocate(n, filerSeek);
            }
        }
        bbs = newFilers;
    }

    @Override
    public long length() {
        return length;
    }

    private ByteBuffer allocate(int index, long length) throws IOException {
        long segmentOffset = maxBufferSegmentSize * index;
        long requiredLength = segmentOffset + length;
        try (RandomAccessFile raf = new RandomAccessFile(file, writeable ? "rw" : "r")) {
            if (requiredLength > raf.length()) {
                raf.seek(requiredLength - 1);
                raf.write(0);
            }
            raf.seek(segmentOffset);
            try (FileChannel channel = raf.getChannel()) {
                return channel.map(writeable ? FileChannel.MapMode.READ_WRITE : MapMode.READ_ONLY,
                    segmentOffset, Math.min(maxBufferSegmentSize, channel.size() - segmentOffset));
            }
        }
    }

    private int read(int bbIndex, int bbSeek) throws IOException {
        if (!hasRemaining(bbIndex, bbSeek, 1)) {
            return -1;
        }
        byte b = bbs[bbIndex].get(bbSeek);
        return b & 0xFF;
    }

    @Override
    public int read(long position) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        int read = read(bbIndex, bbSeek);
        while (read == -1 && bbIndex < bbs.length - 1) {
            bbIndex++;
            read = read(bbIndex, 0);
        }
        return read;
    }

    private int readAtleastOne(long position) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);
        int r = read(bbIndex, bbSeek);
        if (r == -1) {
            throw new EOFException("Failed to fully read 1 byte");
        }
        return r;
    }

    private boolean hasRemaining(int bbIndex, int bbSeek, int length) {
        return bbs[bbIndex].limit() - bbSeek >= length;
    }

    @Override
    public int readInt(long position) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        if (hasRemaining(bbIndex, bbSeek, 4)) {
            return bbs[bbIndex].getInt(bbSeek);
        } else {
            int b0 = readAtleastOne(position);
            int b1 = readAtleastOne(position + 1);
            int b2 = readAtleastOne(position + 2);
            int b3 = readAtleastOne(position + 3);

            int v = 0;
            v |= (b0 & 0xFF);
            v <<= 8;
            v |= (b1 & 0xFF);
            v <<= 8;
            v |= (b2 & 0xFF);
            v <<= 8;
            v |= (b3 & 0xFF);
            return v;
        }
    }

    @Override
    public long readLong(long position) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        if (hasRemaining(bbIndex, bbSeek, 8)) {
            return bbs[bbIndex].getLong(bbSeek);
        } else {
            int b0 = readAtleastOne(position);
            int b1 = readAtleastOne(position + 1);
            int b2 = readAtleastOne(position + 2);
            int b3 = readAtleastOne(position + 3);
            int b4 = readAtleastOne(position + 4);
            int b5 = readAtleastOne(position + 5);
            int b6 = readAtleastOne(position + 6);
            int b7 = readAtleastOne(position + 7);

            long v = 0;
            v |= (b0 & 0xFF);
            v <<= 8;
            v |= (b1 & 0xFF);
            v <<= 8;
            v |= (b2 & 0xFF);
            v <<= 8;
            v |= (b3 & 0xFF);
            v <<= 8;
            v |= (b4 & 0xFF);
            v <<= 8;
            v |= (b5 & 0xFF);
            v <<= 8;
            v |= (b6 & 0xFF);
            v <<= 8;
            v |= (b7 & 0xFF);

            return v;
        }
    }

    private int read(int bbIndex, int bbSeek, byte[] b, int _offset, int _len) throws IOException {
        ByteBuffer bb = bbs[bbIndex];
        int remaining = bb.limit() - bbSeek;
        if (remaining <= 0) {
            return -1;
        }
        int count = Math.min(_len, remaining);
        for (int i = 0; i < count; i++) {
            b[_offset + i] = bb.get(bbSeek + i);
        }
        return count;
    }

    @Override
    public int read(long position, byte[] b, int offset, int len) throws IOException {
        if (len == 0) {
            return 0;
        }

        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        int remaining = len;
        int read = read(bbIndex, bbSeek, b, offset, remaining);
        if (read == -1) {
            read = 0;
        }
        offset += read;
        remaining -= read;
        while (remaining > 0 && bbIndex < bbs.length - 1) {
            bbIndex++;
            bbSeek = 0;
            read = read(bbIndex, bbSeek, b, offset, remaining);
            if (read == -1) {
                read = 0;
            }
            offset += read;
            remaining -= read;
        }
        if (len == remaining) {
            return -1;
        }
        return offset;
    }

    @Override
    public BolBuffer sliceIntoBuffer(long offset, int length, BolBuffer entryBuffer) throws IOException {

        int bbIndex = (int) (offset >> fShift);
        if (bbIndex == (int) (offset + length >> fShift)) {
            int filerSeek = (int) (offset & fseekMask);
            entryBuffer.force(bbs[bbIndex], filerSeek, length);
        } else {
            byte[] rawEntry = new byte[length]; // very rare only on bb boundaries
            read(offset, rawEntry, 0, length);
            entryBuffer.force(rawEntry, 0, length);
        }
        return entryBuffer;
    }

    @Override
    public void close() throws IOException {
        if (bbs.length > 0) {
            ByteBuffer bb = bbs[0];
            if (bb != null) {
                DirectBufferCleaner.clean(bb);
            }
        }
    }


    public void write(long position, byte b) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);
        bbs[bbIndex].put(bbSeek, b);
    }

    public void writeShort(long position, short v) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        if (hasRemaining(bbIndex, bbSeek, 2)) {
            bbs[bbIndex].putShort(bbSeek, v);
        } else {
            write(position, (byte) (v >>> 8));
            write(position + 1, (byte) (v >>> 0));
        }
    }

    public void writeInt(long position, int v) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        if (hasRemaining(bbIndex, bbSeek, 4)) {
            bbs[bbIndex].putInt(bbSeek, v);
        } else {
            write(position, (byte) (v >>> 24));
            write(position + 1, (byte) (v >>> 16));
            write(position + 2, (byte) (v >>> 8));
            write(position + 3, (byte) (v >>> 0));
        }
    }

    public void writeLong(long position, long v) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        if (hasRemaining(bbIndex, bbSeek, 8)) {
            bbs[bbIndex].putLong(bbSeek, v);
        } else {
            write(position, (byte) (v >>> 56));
            write(position + 1, (byte) (v >>> 48));
            write(position + 2, (byte) (v >>> 40));
            write(position + 3, (byte) (v >>> 32));
            write(position + 4, (byte) (v >>> 24));
            write(position + 5, (byte) (v >>> 16));
            write(position + 6, (byte) (v >>> 8));
            write(position + 7, (byte) (v >>> 0));
        }
    }


    public long readVPLong(long position, byte longPrecision) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        if (hasRemaining(bbIndex, bbSeek, longPrecision)) {
            ByteBuffer bb = bbs[bbIndex];
            long v = 0;
            for (int i = 0; i < longPrecision; i++) {
                v |= (bb.get(bbSeek + i) & 0xFF);
                if (i + 1 < longPrecision) {
                    v <<= 8;
                }
            }
            return v;
        } else {
            long v = 0;
            for (int i = 0; i < longPrecision; i++) {
                v |= (readAtleastOne(position + i) & 0xFF);
                if (i + 1 < longPrecision) {
                    v <<= 8;
                }
            }
            return v;
        }
    }


    public void writeVPLong(long position, long v, byte longPrecision) throws IOException {
        int bbIndex = (int) (position >> fShift);
        int bbSeek = (int) (position & fseekMask);

        if (hasRemaining(bbIndex, bbSeek, longPrecision)) {
            ByteBuffer bb = bbs[bbIndex];
            for (int i = 0, shift = (8 * longPrecision) - 8; i < longPrecision; i++, shift -= 8) {
                bb.put(bbSeek + i, (byte) (v >>> shift));
            }
        } else {
            for (int i = 0, shift = (8 * longPrecision) - 8; i < longPrecision; i++, shift -= 8) {
                write(position + i, (byte) (v >>> shift));
            }
        }
    }


    public static void main(String[] args) {

        long r = 123456;
        byte longPrecision = 8;
        byte[] bytes = new byte[longPrecision];

        for (int i = 0, shift = (8 * longPrecision) - 8; i < longPrecision; i++, shift -= 8) {
            bytes[i] = (byte) (r >>> shift);

            System.out.println(i + " " + bytes[i] + " " + shift);
        }

        System.out.println("---");
        long v = 0;
        for (int i = 0; i < longPrecision; i++) {
            v |= (bytes[i] & 0xFF);
            if (i + 1 < longPrecision) {
                v <<= 8;
            }
            System.out.println(i + " " + bytes[i] + " " + v);
        }
        System.out.println(v);
        System.out.println(UIO.bytesLong(bytes));
    }

    /*


     public static long bytesLong(byte[] bytes, int _offset) {
        if (bytes == null) {
            return 0;
        }
        long v = 0;
        v |= (bytes[_offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 1] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 2] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 3] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 4] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 5] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 6] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 7] & 0xFF);
        return v;
    }
     */

}
