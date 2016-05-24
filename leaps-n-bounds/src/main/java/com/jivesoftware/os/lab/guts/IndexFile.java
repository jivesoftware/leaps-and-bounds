package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.io.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.lab.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.ICloseable;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class IndexFile implements ICloseable {

    private final static MetricLogger LOG = MetricLoggerFactory.getLogger();

    static private class OpenFileLock {
    }

    static private class MemMapLock {
    }

    private static final long BUFFER_SEGMENT_SIZE = 1024L * 1024 * 1024;

    private final File file;
    private final String mode;
    private RandomAccessFile randomAccessFile;
    private FileChannel channel;
    private final boolean useMemMap;
    private final AtomicLong size;

    private final AutoGrowingByteBufferBackedFiler memMapFiler;
    private final AtomicLong memMapFilerLength = new AtomicLong(-1);

    private final OpenFileLock openFileLock = new OpenFileLock();
    private final MemMapLock memMapLock = new MemMapLock();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public IndexFile(File file, String mode, boolean useMemMap) throws IOException {
        this.file = file;
        this.mode = mode;
        this.useMemMap = useMemMap;
        this.randomAccessFile = new RandomAccessFile(file, mode);
        this.channel = randomAccessFile.getChannel();
        this.size = new AtomicLong(randomAccessFile.length());
        this.memMapFiler = createMemMap();
    }

    public File getFile() {
        return file;
    }

    public IReadable reader(IReadable current, long requiredLength, boolean fallBackToChannelReader) throws IOException {
        if (closed.get()) {
            throw new IOException("Cannot get a reader from an index that is already closed.");
        }
        if (!useMemMap) {
            if (current != null) {
                return current;
            } else {
                return new IndexFilerChannelReader(this, channel);
            }
        }

        if (current != null && current.length() >= requiredLength) {
            return current;
        }

        long memMapSize = memMapFilerLength.get();
        if (requiredLength > memMapSize) {
            if (fallBackToChannelReader) {
                // mmap is too small, fall back to channel reader
                return new IndexFilerChannelReader(this, channel);
            } else {
                synchronized (memMapLock) {
                    long length = size.get();
                    memMapFiler.seek(length);
                    memMapFilerLength.set(length);
                }
            }
        }
        return memMapFiler.duplicateAll();
    }

    public void flush(boolean fsync) throws IOException {
        if (fsync) {
            randomAccessFile.getFD().sync();
        }
    }

    public IAppendOnly appender() throws IOException {
        if (closed.get()) {
            throw new IOException("Cannot get an appender from an index that is already closed.");
        }
        FileOutputStream writer = new FileOutputStream(file, true);
        return new IAppendOnly() {

            @Override
            public void append(byte[] b, int _offset, int _len) throws IOException {
                writer.write(b, _offset, _len);
                size.addAndGet(_len);
            }

            @Override
            public void flush(boolean fsync) throws IOException {
                IndexFile.this.flush(fsync);
            }

            @Override
            public void close() throws IOException {
                writer.close();
            }

            @Override
            public long length() throws IOException {
                return IndexFile.this.length();
            }

            @Override
            public long getFilePointer() throws IOException {
                return length();
            }

        };
    }

    private AutoGrowingByteBufferBackedFiler createMemMap() throws IOException {
        if (useMemMap) {
            FileBackedMemMappedByteBufferFactory byteBufferFactory = new FileBackedMemMappedByteBufferFactory(file, BUFFER_SEGMENT_SIZE);
            return new AutoGrowingByteBufferBackedFiler(-1L, BUFFER_SEGMENT_SIZE, byteBufferFactory);
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return "IndexFile{"
            + "fileName=" + file
            + ", useMemMap=" + useMemMap
            + ", size=" + size
            + '}';
    }

    @Override
    public void close() throws IOException {
        synchronized (openFileLock) {
            closed.set(true);
            randomAccessFile.close();
        }
    }

    public long length() throws IOException {
        return size.get();
    }

    private void ensureOpen() throws IOException {
        if (closed.get()) {
            throw new IOException("Cannot ensureOpen on an index that is already closed.");
        }
        if (!channel.isOpen()) {
            synchronized (openFileLock) {
                if (closed.get()) {
                    throw new IOException("Cannot ensureOpen on an index that is already closed.");
                }
                if (!channel.isOpen()) {
                    randomAccessFile = new RandomAccessFile(file, mode);
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
