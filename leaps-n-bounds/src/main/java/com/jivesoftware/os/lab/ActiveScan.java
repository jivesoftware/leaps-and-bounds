package com.jivesoftware.os.lab;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.lab.api.RawEntryStream;
import com.jivesoftware.os.lab.api.ScanFromFp;
import com.jivesoftware.os.lab.collections.ConcurrentLHash;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.IOException;
import java.util.Arrays;

import static com.jivesoftware.os.lab.WriteLeapsAndBoundsIndex.ENTRY;
import static com.jivesoftware.os.lab.WriteLeapsAndBoundsIndex.FOOTER;
import static com.jivesoftware.os.lab.WriteLeapsAndBoundsIndex.LEAP;

/**
 *
 * @author jonathan.colt
 */
public class ActiveScan implements ScanFromFp {

    private final Leaps leaps;
    private final ConcurrentLHash<Leaps> leapsCache;
    private final Footer footer;
    private final IReadable readable;
    private final byte[] lengthBuffer;
    private byte[] entryBuffer;
    private long activeFp = Long.MAX_VALUE;
    private boolean activeResult;

    public ActiveScan(Leaps leaps, ConcurrentLHash<Leaps> leapsCache, Footer footer, IReadable readable, byte[] lengthBuffer) {
        this.leaps = leaps;
        this.leapsCache = leapsCache;
        this.footer = footer;
        this.readable = readable;
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
                int length = UIO.readInt(readable, "entryLength", lengthBuffer);
                int entryLength = length - 4;
                if (entryBuffer == null || entryBuffer.length < entryLength) {
                    entryBuffer = new byte[entryLength];
                }
                readable.read(entryBuffer, 0, entryLength);
                activeResult = stream.stream(entryBuffer, 0, entryLength);
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

    public long getInclusiveStartOfRow(byte[] key, boolean exact) throws Exception {
        Leaps at = leaps;
        if (UnsignedBytes.lexicographicalComparator().compare(leaps.lastKey, key) < 0) {
            return -1;
        }
        while (at != null) {
            Leaps next;
            int index = Arrays.binarySearch(at.keys, key, UnsignedBytes.lexicographicalComparator());
            if (index == -(at.fps.length + 1)) {
                /*if (at.fps.length == 0) {
                    return 0;
                }
                return at.fps[at.fps.length - 1] - 1;*/
                return binarySearchClosestFP(at, key, exact);
            } else {
                if (index < 0) {
                    index = -(index + 1);
                }
                next = leapsCache.get(at.fps[index]);
                if (next == null) {
                    readable.seek(at.fps[index]);
                    next = Leaps.read(readable, lengthBuffer);
                    leapsCache.put(at.fps[index], next);
                }
            }
            at = next;
        }
        return -1;
    }

    private long binarySearchClosestFP(Leaps at, byte[] key, boolean exact) throws IOException {
        int low = 0;
        int high = at.startOfEntryIndex.length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long fp = at.startOfEntryIndex[mid];

            readable.seek(fp);
            byte[] midKey = UIO.readByteArray(readable, "key", lengthBuffer);
            if (midKey == null) {
                throw new IllegalStateException("Missing key");
            }

            int cmp = IndexUtil.compare(midKey, 0, midKey.length, key, 0, key.length);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return fp - (1 + 4); // key found. (1 for type 4 for entry length)
            }
        }
        if (exact) {
            return -1;
        } else {
            return at.startOfEntryIndex[low] - (1 + 4); // best index. (1 for type 4 for entry length)
        }
    }

    long count() {
        return footer.count;
    }
}
