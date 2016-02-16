package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.io.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.lab.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.ICloseable;
import com.jivesoftware.os.lab.io.api.IReadable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class IndexFile implements ICloseable {

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
    private final IAppendOnly appendOnly;

    private final AutoGrowingByteBufferBackedFiler memMapFiler;
    private final AtomicLong memMapFilerLength = new AtomicLong(-1);

    private final OpenFileLock openFileLock = new OpenFileLock();
    private final MemMapLock memMapLock = new MemMapLock();

    public IndexFile(File file, String mode, boolean useMemMap) throws IOException {
        this.file = file;
        this.mode = mode;
        this.useMemMap = useMemMap;
        this.randomAccessFile = new RandomAccessFile(file, mode);
        this.channel = randomAccessFile.getChannel();
        this.size = new AtomicLong(randomAccessFile.length());
        this.appendOnly = createAppendOnly();
        this.memMapFiler = createMemMap();
    }

    public File getFile() {
        return file;
    }

    public IReadable reader(IReadable current, long requiredLength, boolean fallBackToChannelReader) throws IOException {
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
        synchronized (memMapLock) {
            long length = size.get();
            if (current != null && current.length() <= length && current.length() >= requiredLength) {
                return current;
            }
            if (fallBackToChannelReader && memMapFilerLength.get() < requiredLength) {
                // mmap is too small, fall back to channel reader
                return new IndexFilerChannelReader(this, channel);
            }
            memMapFiler.seek(length);
            memMapFilerLength.set(length);
        }
        return memMapFiler.duplicateAll();
    }

    public IAppendOnly appender() throws IOException {
        return appendOnly;
    }

    private IAppendOnly createAppendOnly() throws IOException {
        randomAccessFile.seek(size.get());
        FileOutputStream writer = new FileOutputStream(file, true);
        //FileChannel fileChannel = randomAccessFile.getChannel();
        return new IAppendOnly() {
            volatile long position = size.get();

            @Override
            public void append(byte[] b, int _offset, int _len) throws IOException {
                writer.write(b, _offset, _len);
                size.addAndGet(_len);
                //IndexFile.this.write(b, _offset, _len);
                //int wrote = fileChannel.write(ByteBuffer.wrap(b, _offset, _len), position);
                //position = size.addAndGet(wrote);
            }

            @Override
            public void flush(boolean fsync) throws IOException {
                //IndexFile.this.flush(fsync);
                if (fsync) {
                    writer.getFD().sync();
                    //fileChannel.force(false);
                }
            }

            @Override
            public void close() throws IOException {
                writer.close();
                //fileChannel.close();
            }

            @Override
            public long length() throws IOException {
                return IndexFile.this.length();
            }

            @Override
            public long getFilePointer() throws IOException {
                return length();
            }

//            @Override
//            public void write(ByteBuffer... byteBuffers) throws IOException {
//                fileChannel.position(position);
//                long wrote = fileChannel.write(byteBuffers);
//                position = size.addAndGet(wrote);
//            }

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
        randomAccessFile.close();
    }

    public long length() throws IOException {
        return size.get();
    }

    private void ensureOpen() throws IOException {
        if (!channel.isOpen()) {
            synchronized (openFileLock) {
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
