package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

/**
 * @author jonathan.colt
 */
public class IndexFilerChannelReader implements IReadable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final IndexFile parent;
    private FileChannel fc;
    private long fp;

    private final ByteBuffer singleByteBuffer = ByteBuffer.allocateDirect(1);
    private final Object fileLock = new Object();

    public IndexFilerChannelReader(IndexFile parent, FileChannel fc) {
        this.parent = parent;
        this.fc = fc;
    }

    @Override
    public void seek(long position) throws IOException {
        if (position < 0 || position > parent.length()) {
            throw new IOException("seek overflow " + position + " " + this);
        }
        fp = position;
    }

    @Override
    public long length() throws IOException {
        return parent.length();
    }

    @Override
    public long getFilePointer() throws IOException {
        return fp;
    }

    @Override
    public int read() throws IOException {
        while (true) {
            try {
                singleByteBuffer.position(0);
                int read = fc.read(singleByteBuffer, fp);
                fp++;
                singleByteBuffer.position(0);
                return read != 1 ? -1 : singleByteBuffer.get();
            } catch (ClosedChannelException e) {
                ensureOpen();
            }
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public short readShort() throws IOException {
        int v = 0;
        v |= (read() & 0xFF);
        v <<= 8;
        v |= (read() & 0xFF);
        return (short) v;
    }

    @Override
    public int readInt() throws IOException {
        int v = 0;
        v |= (read() & 0xFF);
        v <<= 8;
        v |= (read() & 0xFF);
        v <<= 8;
        v |= (read() & 0xFF);
        v <<= 8;
        v |= (read() & 0xFF);
        return v;
    }

    @Override
    public long readLong() throws IOException {
        long v = 0;
        v |= (read() & 0xFF);
        v <<= 8;
        v |= (read() & 0xFF);
        v <<= 8;
        v |= (read() & 0xFF);
        v <<= 8;
        v |= (read() & 0xFF);
        v <<= 8;
        v |= (read() & 0xFF);
        v <<= 8;
        v |= (read() & 0xFF);
        v <<= 8;
        v |= (read() & 0xFF);
        v <<= 8;
        v |= (read() & 0xFF);
        return v;
    }

    @Override
    public int read(byte[] b, int _offset, int _len) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(b, _offset, _len);
        while (true) {
            try {
                fc.read(bb, fp);
                fp += _len;
                return _len;
            } catch (ClosedChannelException e) {
                ensureOpen();
                bb.position(0);
            }
        }
    }

    @Override
    public ByteBuffer slice(int length) throws IOException {
        throw new UnsupportedOperationException("Cannot slice from a channel reader");
    }

    @Override
    public boolean canSlice(int length) throws IOException {
        return false;
    }

    private void ensureOpen() throws IOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException();
        }
        if (!fc.isOpen()) {
            synchronized (fileLock) {
                if (!fc.isOpen()) {
                    LOG.warn("File channel is closed and must be reopened for {}", parent);
                    fc = parent.getFileChannel();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        fc.close();
    }

}
