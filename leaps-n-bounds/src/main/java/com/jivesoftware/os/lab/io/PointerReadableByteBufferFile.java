package com.jivesoftware.os.lab.io;

import com.jivesoftware.os.lab.io.api.IPointerReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 */
public class PointerReadableByteBufferFile implements IPointerReadable {

    public static final long MAX_BUFFER_SEGMENT_SIZE = UIO.chunkLength(30);
    public static final long MAX_POSITION = MAX_BUFFER_SEGMENT_SIZE * 100;

    private final long initialBufferSegmentSize;
    private final long maxBufferSegmentSize;
    private final File file;

    private ByteBuffer[] bbs;

    private final int fShift;
    private final long fseekMask;

    public PointerReadableByteBufferFile(long initialBufferSegmentSize,
        long maxBufferSegmentSize,
        File file) throws IOException {

        this.initialBufferSegmentSize = initialBufferSegmentSize > 0 ? UIO.chunkLength(UIO.chunkPower(initialBufferSegmentSize, 0)) : -1;
        this.maxBufferSegmentSize = Math.min(UIO.chunkLength(UIO.chunkPower(maxBufferSegmentSize, 0)), MAX_BUFFER_SEGMENT_SIZE);

        this.file = file;
        this.bbs = new ByteBuffer[0];

        // test power of 2
        if ((this.maxBufferSegmentSize & (this.maxBufferSegmentSize - 1)) == 0) {
            this.fShift = Long.numberOfTrailingZeros(this.maxBufferSegmentSize);
            this.fseekMask = this.maxBufferSegmentSize - 1;
        } else {
            throw new IllegalArgumentException("It's hard to ensure powers of 2");
        }
    }

    private ByteBuffer allocate(int index, long length) throws IOException {
        ensureDirectory(file.getParentFile());
        long segmentOffset = maxBufferSegmentSize * index;
        long requiredLength = segmentOffset + length;
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            if (requiredLength > raf.length()) {
                raf.seek(requiredLength - 1);
                raf.write(0);
            }
            raf.seek(segmentOffset);
            try (FileChannel channel = raf.getChannel()) {
                return channel.map(FileChannel.MapMode.READ_WRITE, segmentOffset, Math.min(maxBufferSegmentSize, channel.size() - segmentOffset));
            }
        }
    }

    private ByteBuffer reallocate(int index, long newSize) throws IOException {
        return allocate(index, newSize);
    }

    private long nextLength(int index) {
        long segmentOffset = maxBufferSegmentSize * index;
        return Math.min(maxBufferSegmentSize, file.length() - segmentOffset);
    }

    private void ensureDirectory(File directory) throws IOException {
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                if (!directory.exists()) {
                    throw new IOException("Failed to create directory: " + directory);
                }
            }
        }
    }

    private int position(int f, long fseek) throws IOException {

        if (f >= bbs.length) {
            int lastFilerIndex = bbs.length - 1;
            if (lastFilerIndex > -1 && bbs[lastFilerIndex].capacity() < maxBufferSegmentSize) {
                ByteBuffer reallocate = reallocate(lastFilerIndex, maxBufferSegmentSize);
                bbs[lastFilerIndex] = reallocate;
            }

            int newLength = f + 1;
            ByteBuffer[] newFilers = new ByteBuffer[newLength];
            System.arraycopy(bbs, 0, newFilers, 0, bbs.length);
            for (int n = bbs.length; n < newLength; n++) {
                if (n < newLength - 1) {
                    newFilers[n] = allocate(n, maxBufferSegmentSize);
                } else {
                    newFilers[n] = allocate(n, Math.max(fseek, initialBufferSegmentSize));
                }
            }
            bbs = newFilers;

        } else if (f == bbs.length - 1 && fseek > bbs[f].capacity()) {
            long newSize = nextLength(f);
            ByteBuffer reallocate = reallocate(f, Math.min(maxBufferSegmentSize, newSize));
            bbs[f] = reallocate;
        }
        return f;

    }

    @Override
    public int read(long position) throws IOException {
        if (position > MAX_POSITION) {
            throw new IllegalStateException("Encountered a likely runaway file position! position=" + position);
        }
        int filerIndex = (int) (position >> fShift);
        long filerSeek = position & fseekMask;

        filerIndex = position(filerIndex, filerSeek);

        int read = bbs[filerIndex].get((int) filerSeek);
        while (read == -1 && filerIndex < bbs.length - 1) {
            filerIndex++;
            filerSeek = 0;
            read = bbs[filerIndex].get((int) filerSeek);
        }
        return read;
    }

    private int readAtleastOne(long position) throws IOException {
        int r = read(position);
        if (r == -1) {
            throw new EOFException("Failed to fully read 1 byte");
        }
        return r;
    }

    @Override
    public int readInt(long position) throws IOException {
        if (position > MAX_POSITION) {
            throw new IllegalStateException("Encountered a likely runaway file position! position=" + position);
        }
        int filerIndex = (int) (position >> fShift);
        long filerSeek = position & fseekMask;

        filerIndex = position(filerIndex, filerSeek);

        if (filerSeek + 4 < bbs[filerIndex].capacity()) {
            return bbs[filerIndex].getInt((int) filerSeek);
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
        if (position > MAX_POSITION) {
            throw new IllegalStateException("Encountered a likely runaway file position! position=" + position);
        }
        int filerIndex = (int) (position >> fShift);
        long filerSeek = position & fseekMask;

        filerIndex = position(filerIndex, filerSeek);

        if (filerSeek + 8 < bbs[filerIndex].capacity()) {
            return bbs[(int) filerIndex].getLong((int) filerSeek);
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

    @Override
    public int read(long position, byte[] b, int offset, int len) throws IOException {
        if (position > MAX_POSITION) {
            throw new IllegalStateException("Encountered a likely runaway file position! position=" + position);
        }
        int bbIndex = (int) (position >> fShift);
        long bbSeek = position & fseekMask;

        bbIndex = position(bbIndex, bbSeek);

        if (len == 0) {
            return 0;
        }
        int remaining = len;
        int read = Math.max(-1, (int) (bbs[bbIndex].capacity() - bbSeek));
        for (int i = 0; i < read; i++) {
            b[offset] = bbs[bbIndex].get((int) bbSeek);
            offset++;
            bbSeek++;
        }
        if (read == -1) {
            read = 0;
        }
        offset += read;
        remaining -= read;
        while (remaining > 0 && bbIndex < bbs.length - 1) {
            bbIndex++;
            bbSeek = 0;

            read = Math.max(-1, (int) (bbs[bbIndex].capacity() - bbSeek));
            for (int i = 0; i < read; i++) {
                b[offset] = bbs[bbIndex].get((int) bbSeek);
                offset++;
                bbSeek++;
            }
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
    public void close() throws IOException {
        if (bbs.length > 0) {
            ByteBuffer bb = bbs[0];
            if (bb != null) {
                DirectBufferCleaner.clean(bb);
            }
        }
    }
}
