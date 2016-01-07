package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.io.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.lab.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.lab.io.HeapFiler;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.ICloseable;
import com.jivesoftware.os.lab.io.api.IReadable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class IndexFile implements ICloseable {

    private static final long BUFFER_SEGMENT_SIZE = 1024L * 1024 * 1024;

    private final String fileName;
    private final String mode;
    private RandomAccessFile randomAccessFile;
    private FileChannel channel;
    private final boolean useMemMap;
    private final AtomicLong size;
    private final IAppendOnly appendOnly;

    private AutoGrowingByteBufferBackedFiler memMapFiler;
    private final AtomicLong memMapFilerLength = new AtomicLong(-1);

    private final Object fileLock = new Object();
    private final Object memMapLock = new Object();

    public IndexFile(String fileName, String mode, boolean useMemMap, int appendBufferSize) throws IOException {
        this.fileName = fileName;
        this.mode = mode;
        this.useMemMap = useMemMap;
        this.randomAccessFile = new RandomAccessFile(fileName, mode);
        this.channel = randomAccessFile.getChannel();
        this.size = new AtomicLong(randomAccessFile.length());
        this.appendOnly = createAppendOnly(appendBufferSize);
        this.memMapFiler = createMemMap();
    }

    public String getFileName() {
        return fileName;
    }

    public IReadable reader(IReadable current, long requiredLength, int bufferSize) throws IOException {
        if (!useMemMap) {
            if (current != null) {
                return current;
            } else {
                IndexFilerChannelReader reader = new IndexFilerChannelReader(this, channel);
                return bufferSize > 0 ? new HeapBufferedReadable(reader, bufferSize) : reader;
            }
        }

        if (current != null && current.length() >= requiredLength) {
            return current;
        }
        synchronized (memMapLock) {
            long length = size.get();
            memMapFiler.seek(length);
            memMapFilerLength.set(length);
        }
        return memMapFiler.duplicateAll();
    }

    public IAppendOnly appender() throws IOException {
        return appendOnly;
    }

    private IAppendOnly createAppendOnly(int bufferSize) throws IOException {
        HeapFiler filer = bufferSize > 0 ? new HeapFiler(bufferSize) : null;
        randomAccessFile.seek(size.get());
        return new IAppendOnly() {

            @Override
            public void write(byte[] b, int _offset, int _len) throws IOException {
                if (filer != null) {
                    filer.write(b, _offset, _len);
                    if (filer.length() > bufferSize) {
                        flush(false);
                    }
                } else {
                    IndexFile.this.write(b, _offset, _len);
                }
            }

            @Override
            public void flush(boolean fsync) throws IOException {
                if (filer != null && filer.length() > 0) {
                    long length = filer.length();
                    IndexFile.this.write(filer.leakBytes(), 0, (int) length);
                    filer.reset();
                }
                IndexFile.this.flush(fsync);
            }

            @Override
            public void close() throws IOException {
                if (filer != null) {
                    filer.reset();
                }
            }

            @Override
            public Object lock() {
                return IndexFile.this;
            }

            @Override
            public long length() throws IOException {
                return IndexFile.this.length() + (filer != null ? filer.length() : 0);
            }

            @Override
            public long getFilePointer() throws IOException {
                return length();
            }
        };
    }

    private AutoGrowingByteBufferBackedFiler createMemMap() throws IOException {
        if (useMemMap) {
            FileBackedMemMappedByteBufferFactory byteBufferFactory = new FileBackedMemMappedByteBufferFactory(new File(fileName), BUFFER_SEGMENT_SIZE);
            return new AutoGrowingByteBufferBackedFiler(-1L, BUFFER_SEGMENT_SIZE, byteBufferFactory);
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return "IndexFile{"
            + "fileName=" + fileName
            + ", useMemMap=" + useMemMap
            + ", size=" + size
            + '}';
    }

    @Override
    public void close() throws IOException {
        randomAccessFile.close();
    }

    public long length() throws IOException {
        return size.get();
    }

    private void write(byte[] b, int _offset, int _len) throws IOException {
        while (true) {
            try {
                randomAccessFile.write(b, _offset, _len);
                size.addAndGet(_len);
                break;
            } catch (ClosedChannelException e) {
                ensureOpen();
                randomAccessFile.seek(size.get());
            }
        }
    }

    private void flush(boolean fsync) throws IOException {
        if (fsync) {
            while (true) {
                try {
                    randomAccessFile.getFD().sync();
                    break;
                } catch (ClosedChannelException e) {
                    ensureOpen();
                }
            }
        }
    }

    public void truncate(long size) throws IOException {
        // should only be called with a write AND a read lock
        while (true) {
            try {
                randomAccessFile.setLength(size);

                synchronized (memMapLock) {
                    memMapFiler = createMemMap();
                    memMapFilerLength.set(-1);
                }
                break;
            } catch (ClosedChannelException e) {
                ensureOpen();
            }
        }
    }

    private void ensureOpen() throws IOException {
        if (!channel.isOpen()) {
            synchronized (fileLock) {
                if (!channel.isOpen()) {
                    randomAccessFile = new RandomAccessFile(fileName, mode);
                    channel = randomAccessFile.getChannel();
                }
            }
        }
    }

    FileChannel getFileChannel() throws IOException {
        ensureOpen();
        return channel;
    }
}
