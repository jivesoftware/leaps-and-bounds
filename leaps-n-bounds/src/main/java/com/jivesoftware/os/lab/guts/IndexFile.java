package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.LABIndexClosedException;
import com.jivesoftware.os.lab.io.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.lab.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.ICloseable;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.DataOutputStream;
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
    private final AtomicLong size;

    private final AutoGrowingByteBufferBackedFiler memMapFiler;
    private final AtomicLong memMapFilerLength = new AtomicLong(-1);

    private final OpenFileLock openFileLock = new OpenFileLock();
    private final MemMapLock memMapLock = new MemMapLock();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public IndexFile(File file, String mode) throws IOException {
        this.file = file;
        this.mode = mode;
        this.randomAccessFile = new RandomAccessFile(file, mode);
        this.channel = randomAccessFile.getChannel();
        this.size = new AtomicLong(randomAccessFile.length());
        this.memMapFiler = createMemMap();
    }

    public String getFileName() {
        return file.toString();
    }

    public void delete() {
        file.delete();
    }

    public boolean isClosed() {
        return closed.get();
    }

    public IReadable reader(IReadable current, long requiredLength) throws Exception {
        if (closed.get()) {
            throw new LABIndexClosedException("Cannot get a reader from an index that is already closed.");
        }
       
        if (current != null && current.length() >= requiredLength) {
            return current;
        }

        long memMapSize = memMapFilerLength.get();
        if (requiredLength > memMapSize) {
            synchronized (memMapLock) {
                long length = size.get();
                memMapFiler.seek(length);
                memMapFilerLength.set(length);
            }
        }
        return memMapFiler.duplicateAll();
    }

    public void flush(boolean fsync) throws IOException {
        if (fsync) {
            randomAccessFile.getFD().sync();
        }
    }

    public IAppendOnly appender() throws Exception {
        if (closed.get()) {
            throw new LABIndexClosedException("Cannot get an appender from an index that is already closed.");
        }
        DataOutputStream writer = new DataOutputStream(new FileOutputStream(file, true));
        return new IAppendOnly() {
            @Override
            public void appendByte(byte b) throws IOException {
                writer.writeByte(b);
                size.addAndGet(1);
            }

            @Override
            public void appendShort(short s) throws IOException {
                writer.writeShort(s);
                size.addAndGet(2);
            }

            @Override
            public void appendInt(int i) throws IOException {
                writer.writeInt(i);
                size.addAndGet(4);
            }

            @Override
            public void appendLong(long l) throws IOException {
                writer.writeLong(l);
                size.addAndGet(8);
            }

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
        FileBackedMemMappedByteBufferFactory byteBufferFactory = new FileBackedMemMappedByteBufferFactory(file, BUFFER_SEGMENT_SIZE);
        return new AutoGrowingByteBufferBackedFiler(-1L, BUFFER_SEGMENT_SIZE, byteBufferFactory);
    }

    @Override
    public String toString() {
        return "IndexFile{"
            + "fileName=" + file
            + ", size=" + size
            + '}';
    }

    @Override
    public void close() throws IOException {
        synchronized (openFileLock) {
            if (closed.compareAndSet(false, true)) {
                randomAccessFile.close();
                if (memMapFiler != null) {
                    memMapFiler.close();
                }
            }
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
                    try {
                        randomAccessFile.close();
                    } catch (IOException e) {
                        LOG.error("Failed to close existing random access file while reacquiring channel");
                    }
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
