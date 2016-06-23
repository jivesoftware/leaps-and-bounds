package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ScanFromFp;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.LongBuffer;
import java.util.Arrays;

import static com.jivesoftware.os.lab.guts.LABAppendableIndex.ENTRY;
import static com.jivesoftware.os.lab.guts.LABAppendableIndex.FOOTER;
import static com.jivesoftware.os.lab.guts.LABAppendableIndex.LEAP;

/**
 * @author jonathan.colt
 */
public class ActiveScan implements ScanFromFp {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final Rawhide rawhide;
    private final RawEntryFormat rawEntryFormat;
    private final Leaps leaps;
    private final long cacheKey;
    private final LRUConcurrentBAHLinkedHash<Leaps> leapsCache;
    private final Footer footer;
    private final IReadable readable;
    private final byte[] lengthBuffer;
    private final byte[] cacheKeyBuffer;
    private byte[] entryBuffer;
    private long activeFp = Long.MAX_VALUE;
    private boolean activeResult;

    public ActiveScan(Rawhide rawhide,
        RawEntryFormat rawEntryFormat,
        Leaps leaps,
        long cacheKey,
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache,
        Footer footer,
        IReadable readable,
        byte[] cacheKeyBuffer,
        byte[] lengthBuffer) {

        this.rawhide = rawhide;
        this.rawEntryFormat = rawEntryFormat;
        this.leaps = leaps;
        this.cacheKey = cacheKey;
        this.leapsCache = leapsCache;
        this.footer = footer;
        this.readable = readable;
        this.cacheKeyBuffer = cacheKeyBuffer;
        this.lengthBuffer = lengthBuffer;
    }

    @Override
    public boolean next(long fp, RawEntryStream stream) throws Exception {
        if (activeFp == Long.MAX_VALUE || activeFp != fp) {
            activeFp = fp;
            readable.seek(fp);
        }
        activeResult = false;
        int type;
        while ((type = readable.read()) >= 0) {
            if (type == ENTRY) {
                int entryLength = rawhide.entryLength(rawEntryFormat, readable, lengthBuffer);
                if (entryBuffer == null || entryBuffer.length < entryLength) {
                    entryBuffer = new byte[entryLength];
                }
                readable.read(entryBuffer, 0, entryLength);
                activeResult = stream.stream(rawEntryFormat, entryBuffer, 0, entryLength);
                return false;
            } else if (type == FOOTER) {
                activeResult = false;
                return false;
            } else if (type == LEAP) {
                int length = UIO.readInt(readable, "entryLength", lengthBuffer);
                readable.seek(readable.getFilePointer() + (length - 4));
            } else {
                throw new IllegalStateException("Bad row type:" + type + " at fp:" + (readable.getFilePointer() - 1));
            }
        }
        throw new IllegalStateException("Missing footer");
    }

    @Override
    public boolean result() {
        return activeResult;
    }

    @Override
    public void reset() {
        activeFp = Long.MAX_VALUE;
        activeResult = false;
    }

    public long getInclusiveStartOfRow(byte[] key, boolean exact, byte[] intBuffer) throws Exception {
        Leaps at = leaps;
        long rowIndex = -1;
        if (rawhide.compare(leaps.lastKey, key) < 0) {
            return rowIndex;
        }
        int cacheMisses = 0;
        int cacheHits = 0;
        while (at != null) {
            Leaps next;
            int index = Arrays.binarySearch(at.keys, key, rawhide);
            if (index == -(at.fps.length + 1)) {
                rowIndex = binarySearchClosestFP(at, 0, key, exact, intBuffer);
                break;
            } else {
                if (index < 0) {
                    index = -(index + 1);
                }

                UIO.longBytes(cacheKey, cacheKeyBuffer, 0);
                UIO.longBytes(at.fps[index], cacheKeyBuffer, 8);

                next = leapsCache.get(cacheKeyBuffer);
                if (next == null) {
                    readable.seek(at.fps[index]);
                    next = Leaps.read(readable, lengthBuffer);
                    leapsCache.put(Arrays.copyOf(cacheKeyBuffer, 16), next);
                    cacheMisses++;
                } else {
                    cacheHits++;
                }
            }
            at = next;
        }

        LOG.inc("LAB>leapCache>calls");
        if (cacheHits > 0) {
            LOG.inc("LAB>leapCache>hits", cacheHits);
        }
        if (cacheMisses > 0) {
            LOG.inc("LAB>leapCache>misses", cacheMisses);
        }
        return rowIndex;
    }

    private long binarySearchClosestFP(Leaps at, long keyFormat, byte[] key, boolean exact, byte[] intBuffer) throws Exception {
        LongBuffer startOfEntryBuffer = at.startOfEntry.get(readable);
        int low = 0;
        int high = startOfEntryBuffer.limit() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long fp = startOfEntryBuffer.get(mid);

            readable.seek(fp + 1); // skip 1 type byte

            int cmp = rawhide.compareKeyFromEntry(rawEntryFormat, readable, keyFormat, key, 0, key.length, intBuffer);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return fp;
            }
        }
        if (exact) {
            return -1;
        } else {
            return startOfEntryBuffer.get(low);
        }
    }

    long count() {
        return footer.count;
    }
}
