package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.GetRaw;
import com.jivesoftware.os.lab.api.NextRawEntry;
import com.jivesoftware.os.lab.api.ReadIndex;
import com.jivesoftware.os.lab.collections.ConcurrentLHash;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jonathan.colt
 */
public class ReadLeapsAndBoundsIndex implements ReadIndex {

    private final AtomicBoolean disposed;
    private final Semaphore hideABone;
    private final Leaps leaps;
    private final ConcurrentLHash<Leaps> leapsCache;
    private final Footer footer;
    private final Callable<IReadable> readable;

    public ReadLeapsAndBoundsIndex(AtomicBoolean disposed,
        Semaphore hideABone,
        Leaps leaps,
        ConcurrentLHash<Leaps> leapsCache,
        Footer footer,
        Callable<IReadable> readable) {
        this.disposed = disposed;
        this.hideABone = hideABone;
        this.leaps = leaps;
        this.leapsCache = leapsCache;
        this.footer = footer;
        this.readable = readable;
    }

    @Override
    public void acquire() throws Exception {
        hideABone.acquire();
        if (disposed.get()) {
            hideABone.release();
            throw new IllegalStateException("Cannot acquire a bone on a disposed index.");
        }
    }

    @Override
    public void release() {
        hideABone.release();
    }

    @Override
    public GetRaw get() throws Exception {
        return new Gets(new ActiveScan(leaps, leapsCache, footer, readable.call(), new byte[8]));
    }

    @Override
    public NextRawEntry rangeScan(byte[] from, byte[] to) throws Exception {
        ActiveScan activeScan = new ActiveScan(leaps, leapsCache, footer, readable.call(), new byte[8]);
        activeScan.reset();
        long fp = activeScan.getInclusiveStartOfRow(from, false);
        if (fp < 0) {
            return (stream) -> false;
        }
        return (stream) -> {
            boolean[] once = new boolean[]{false};
            boolean more = true;
            while (!once[0] && more) {
                more = activeScan.next(fp,
                    (rawEntry, offset, length) -> {
                        int keylength = UIO.bytesInt(rawEntry, offset);
                        int c = IndexUtil.compare(rawEntry, 4, keylength, from, 0, from.length);
                        if (c >= 0) {
                            c = IndexUtil.compare(rawEntry, 4, keylength, to, 0, to.length);
                            if (c < 0) {
                                once[0] = true;
                            }
                            return c < 0 && stream.stream(rawEntry, offset, length);
                        } else {
                            return true;
                        }
                    });
            }
            return activeScan.result();
        };
    }

    @Override
    public NextRawEntry rowScan() throws Exception {
        ActiveScan activeScan = new ActiveScan(leaps, leapsCache, footer, readable.call(), new byte[8]);
        activeScan.reset();
        return (stream) -> {
            activeScan.next(0, stream);
            return activeScan.result();
        };
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public long count() throws Exception {
        return footer.count;
    }

    @Override
    public boolean isEmpty() throws Exception {
        return footer.count == 0;
    }

}
