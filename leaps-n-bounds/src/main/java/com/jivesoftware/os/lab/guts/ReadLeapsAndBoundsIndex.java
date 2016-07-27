package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.NextRawEntry.Next;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.io.api.IReadable;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

/**
 * @author jonathan.colt
 */
public class ReadLeapsAndBoundsIndex implements ReadIndex {

    private final Semaphore hideABone;
    private final Rawhide rawhide;
    private final FormatTransformer readKeyFormatTransormer;
    private final FormatTransformer readValueFormatTransormer;
    private final Leaps leaps;
    private final long cacheKey;
    private final LRUConcurrentBAHLinkedHash<Leaps> leapsCache;
    private final Footer footer;
    private final Callable<IReadable> readable;

    public ReadLeapsAndBoundsIndex(Semaphore hideABone,
        Rawhide rawhide,
        FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        Leaps leaps,
        long cacheKey,
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache,
        Footer footer,
        Callable<IReadable> readable) {
        this.hideABone = hideABone;
        this.rawhide = rawhide;
        this.readKeyFormatTransormer = readKeyFormatTransormer;
        this.readValueFormatTransormer = readValueFormatTransormer;
        this.leaps = leaps;
        this.cacheKey = cacheKey;
        this.leapsCache = leapsCache;
        this.footer = footer;
        this.readable = readable;
    }

    @Override
    public String toString() {
        return "ReadLeapsAndBoundsIndex{" + "footer=" + footer + '}';
    }

    @Override
    public void release() {
        hideABone.release();
    }

    @Override
    public GetRaw get() throws Exception {
        ActiveScan activeScan = new ActiveScan(rawhide,
            readKeyFormatTransormer,
            readValueFormatTransormer,
            leaps,
            cacheKey,
            leapsCache,
            footer,
            readable.call(),
            new byte[16]);
        // TODO re-eval if we need to do the readabe.call() and the ActiveScan initialization
        return new Gets(activeScan);
    }

    @Override
    public NextRawEntry rangeScan(byte[] from, byte[] to) throws Exception {
        ActiveScan activeScan = new ActiveScan(rawhide,
            readKeyFormatTransormer,
            readValueFormatTransormer,
            leaps,
            cacheKey,
            leapsCache,
            footer,
            readable.call(),
            new byte[16]);
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
                    (readKeyFormatTransormer, readValueFormatTransormer, rawEntry, offset, length) -> {
                        int c = rawhide.compareKey(readKeyFormatTransormer, readValueFormatTransormer, rawEntry, offset, from, 0, from.length);
                        if (c >= 0) {
                            c = to == null ? -1 : rawhide.compareKey(readKeyFormatTransormer, readValueFormatTransormer, rawEntry, offset, to, 0, to.length);
                            if (c < 0) {
                                once[0] = true;
                            }
                            return c < 0 && stream.stream(readKeyFormatTransormer, readValueFormatTransormer, rawEntry, offset, length);
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
        ActiveScan activeScan = new ActiveScan(rawhide,
            readKeyFormatTransormer,
            readValueFormatTransormer,
            leaps,
            cacheKey,
            leapsCache,
            footer,
            readable.call(),
            new byte[16]);
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
