package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.jive.utils.collections.lh.ConcurrentLHash;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.NextRawEntry.Next;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
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
    public boolean acquire() throws InterruptedException {
        hideABone.acquire();
        if (disposed.get()) {
            hideABone.release();
            return false;
        }
        return true;
    }

    @Override
    public void release() {
        hideABone.release();
    }

    @Override
    public GetRaw get() throws Exception {

        // TODO re-eval if we need to do the readabe.call() and the ActiveScan initialization
        return new Gets(new ActiveScan(leaps, leapsCache, footer, readable.call(), new byte[8]));
    }

    @Override
    public NextRawEntry rangeScan(byte[] from, byte[] to) throws Exception {
        ActiveScan activeScan = new ActiveScan(leaps, leapsCache, footer, readable.call(), new byte[8]);
        activeScan.reset();
        long fp = activeScan.getInclusiveStartOfRow(from, false);
        if (fp < 0) {
            return (stream) -> Next.eos;
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
                            c = to == null ? -1 : IndexUtil.compare(rawEntry, 4, keylength, to, 0, to.length);
                            if (c < 0) {
                                once[0] = true;
                            }
                            return c < 0 && stream.stream(rawEntry, offset, length);
                        } else {
                            return true;
                        }
                    });
            }
            more = activeScan.result();
            return more ? Next.more : Next.stopped;
        };
    }

    @Override
    public NextRawEntry rowScan() throws Exception {
        ActiveScan activeScan = new ActiveScan(leaps, leapsCache, footer, readable.call(), new byte[8]);
        activeScan.reset();
        return (stream) -> {
            activeScan.next(0, stream);
            boolean more = activeScan.result();
            return more ? Next.more : Next.stopped;
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
