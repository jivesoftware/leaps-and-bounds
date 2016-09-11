package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ScanFromFp;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Comparator;

import static com.jivesoftware.os.lab.guts.LABAppendableIndex.ENTRY;
import static com.jivesoftware.os.lab.guts.LABAppendableIndex.FOOTER;
import static com.jivesoftware.os.lab.guts.LABAppendableIndex.LEAP;

/**
 * @author jonathan.colt
 */
public class ActiveScan implements ScanFromFp {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final Rawhide rawhide;
    private final FormatTransformer readKeyFormatTransormer;
    private final FormatTransformer readValueFormatTransormer;
    private final Leaps leaps;
    private final long cacheKey;
    private final LRUConcurrentBAHLinkedHash<Leaps> leapsCache;
    private final Footer footer;
    private final IReadable readable;
    private final byte[] cacheKeyBuffer;
    private long activeFp = Long.MAX_VALUE;
    private boolean activeResult;

    public ActiveScan(Rawhide rawhide,
        FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        Leaps leaps,
        long cacheKey,
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache,
        Footer footer,
        IReadable readable,
        byte[] cacheKeyBuffer) {

        this.rawhide = rawhide;
        this.readKeyFormatTransormer = readKeyFormatTransormer;
        this.readValueFormatTransormer = readValueFormatTransormer;
        this.leaps = leaps;
        this.cacheKey = cacheKey;
        this.leapsCache = leapsCache;
        this.footer = footer;
        this.readable = readable;
        this.cacheKeyBuffer = cacheKeyBuffer;
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
                int entryLength = rawhide.rawEntryLength(readable);
                if (readable.canSlice(entryLength)) {
                    ByteBuffer entry = readable.slice(entryLength);
                    activeResult = stream.stream(readKeyFormatTransormer, readValueFormatTransormer, entry);
                } else {
                    byte[] rawEntry = new byte[entryLength]; // very rare only on bb boundaries
                    readable.read(rawEntry, 0, entryLength);
                    activeResult = stream.stream(readKeyFormatTransormer, readValueFormatTransormer, ByteBuffer.wrap(rawEntry));
                }
                return false;
            } else if (type == FOOTER) {
                activeResult = false;
                return false;
            } else if (type == LEAP) {
                int length = readable.readInt(); // entryLength
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

    public long getInclusiveStartOfRow(byte[] key, boolean exact) throws Exception {
        Leaps l = leaps;
        long rowIndex = -1;
        ByteBuffer bbKey = ByteBuffer.wrap(key);

        if (rawhide.compareKeys(l.lastKey, bbKey) < 0) {
            return rowIndex;
        }
        Comparator<ByteBuffer> byteBufferKeyComparator = rawhide.getByteBufferKeyComparator();
        int cacheMisses = 0;
        int cacheHits = 0;
        while (l != null) {
            Leaps next;
            int index = Arrays.binarySearch(l.keys, bbKey, byteBufferKeyComparator);
            if (index == -(l.fps.length + 1)) {
                rowIndex = binarySearchClosestFP(rawhide, readKeyFormatTransormer, readValueFormatTransormer, readable, l, bbKey, exact);
                break;
            } else {
                if (index < 0) {
                    index = -(index + 1);
                }

                UIO.longBytes(cacheKey, cacheKeyBuffer, 0);
                UIO.longBytes(l.fps[index], cacheKeyBuffer, 8);

                next = leapsCache.get(cacheKeyBuffer);
                if (next == null) {
                    readable.seek(l.fps[index]);
                    next = Leaps.read(readKeyFormatTransormer, readable);
                    leapsCache.put(Arrays.copyOf(cacheKeyBuffer, 16), next);
                    cacheMisses++;
                } else {
                    cacheHits++;
                }
            }
            l = next;
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

    private static long binarySearchClosestFP(Rawhide rawhide,
        FormatTransformer readKeyFormatTransormer,
        FormatTransformer readValueFormatTransormer,
        IReadable readable,
        Leaps leaps,
        ByteBuffer key,
        boolean exact) throws Exception {

        LongBuffer startOfEntryBuffer = leaps.startOfEntry.get(readable);
        int low = 0;
        int high = startOfEntryBuffer.limit() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long fp = startOfEntryBuffer.get(mid);

            readable.seek(fp + 1); // skip 1 type byte

            int cmp = rawhide.compareKeyFromEntry(readKeyFormatTransormer, readValueFormatTransormer, readable, key);
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
            try {
                return startOfEntryBuffer.get(low);
            } catch (Exception x) {
                throw x;
            }
        }
    }

    long count() {
        return footer.count;
    }
}
