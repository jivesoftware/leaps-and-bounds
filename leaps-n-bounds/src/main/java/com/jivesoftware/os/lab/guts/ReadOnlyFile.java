package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.io.PointerReadableByteBufferFile;
import com.jivesoftware.os.lab.io.api.IPointerReadable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jonathan.colt
 */
public class ReadOnlyFile {

//    private final static MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final long BUFFER_SEGMENT_SIZE = 1024L * 1024 * 1024;

    private final File file;
    private final RandomAccessFile randomAccessFile;
//    private FileChannel channel;
    private final long size;

    private volatile IPointerReadable pointerReadable;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ReadOnlyFile(File file) throws IOException {
        this.file = file;
        this.randomAccessFile = new RandomAccessFile(file, "r");
//        this.channel = randomAccessFile.getChannel();
        this.size = randomAccessFile.length();
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

    public IPointerReadable pointerReadable(long bufferSegmentSize) throws IOException {
        if (pointerReadable == null) {
            pointerReadable = new PointerReadableByteBufferFile(bufferSegmentSize > 0 ? bufferSegmentSize : BUFFER_SEGMENT_SIZE, file);
        }
        return pointerReadable;
    }

    public void flush(boolean fsync) throws IOException {
        if (fsync) {
            randomAccessFile.getFD().sync();
        }
    }

    @Override
    public String toString() {
        return "ReadOnlyFile{"
            + "fileName=" + file
            + ", size=" + size
            + '}';
    }

    public void close() throws IOException {
        synchronized (closed) {
            if (closed.compareAndSet(false, true)) {
                randomAccessFile.close();
                if (pointerReadable != null) {
                    pointerReadable.close();
                }
            }
        }
    }

    public long length() throws IOException {
        return size;
    }

//    private void ensureOpen() throws IOException {
//        if (closed.get()) {
//            throw new IOException("Cannot ensureOpen on an index that is already closed.");
//        }
//        if (!channel.isOpen()) {
//            synchronized (closed) {
//                if (closed.get()) {
//                    throw new IOException("Cannot ensureOpen on an index that is already closed.");
//                }
//                if (!channel.isOpen()) {
//                    try {
//                        randomAccessFile.close();
//                    } catch (IOException e) {
//                        LOG.error("Failed to close existing random access file while reacquiring channel");
//                    }
//                    randomAccessFile = new RandomAccessFile(file, "r");
//                    channel = randomAccessFile.getChannel();
//                }
//            }
//        }
//    }
//
//    FileChannel getFileChannel() throws IOException {
//        ensureOpen();
//        return channel;
//    }
}
