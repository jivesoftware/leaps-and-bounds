package com.jivesoftware.os.lab.guts;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.NextRawEntry.Next;
import com.jivesoftware.os.lab.guts.api.RawAppendableIndex;
import com.jivesoftware.os.lab.guts.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.lab.guts.api.RawEntries;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class RawMemoryIndex implements RawAppendableIndex, RawConcurrentReadableIndex, RawEntries {

    private final ConcurrentSkipListMap<byte[], byte[]> index = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
    private final AtomicLong approximateCount = new AtomicLong();
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    private final ExecutorService destroy;
    private final Rawhide rawhide;

    private final int numBones = Short.MAX_VALUE;
    private final Semaphore hideABone = new Semaphore(numBones, true);
    private final ReadIndex reader;
    private AtomicReference<TimestampAndVersion> maxTimestampAndVersion = new AtomicReference<>(TimestampAndVersion.NULL);

    public RawMemoryIndex(ExecutorService destroy, Rawhide rawhide) {
        this.destroy = destroy;
        this.rawhide = rawhide;
        this.reader = new ReadIndex() {

            @Override
            public GetRaw get() throws Exception {

                return new GetRaw() {

                    private boolean result;

                    @Override
                    public boolean get(byte[] key, RawEntryStream stream) throws Exception {
                        byte[] rawEntry = index.get(key);
                        if (rawEntry == null) {
                            return false;
                        } else {
                            result = stream.stream(rawEntry, 0, rawEntry.length);
                            return true;
                        }
                    }

                    @Override
                    public boolean result() {
                        return result;
                    }
                };
            }

            @Override
            public NextRawEntry rangeScan(byte[] from, byte[] to) throws Exception {
                Iterator<Map.Entry<byte[], byte[]>> iterator;
                if (from != null && to != null) {
                    iterator = index.subMap(from, true, to, false).entrySet().iterator();
                } else if (from != null) {
                    iterator = index.tailMap(from, true).entrySet().iterator();
                } else if (to != null) {
                    iterator = index.headMap(to, false).entrySet().iterator();
                } else {
                    iterator = index.entrySet().iterator();
                }
                return (stream) -> {
                    if (iterator.hasNext()) {
                        Map.Entry<byte[], byte[]> next = iterator.next();
//                        System.out.println("readToWrite:" + UIO.bytesLong(next.getKey()));

                        boolean more = stream.stream(next.getValue(), 0, next.getValue().length);
                        return more ? Next.more : Next.stopped;
                    }
                    return Next.eos;
                };
            }

            @Override
            public NextRawEntry rowScan() throws Exception {
                Iterator<Map.Entry<byte[], byte[]>> iterator = index.entrySet().iterator();
                return (stream) -> {
                    if (iterator.hasNext()) {
                        Map.Entry<byte[], byte[]> next = iterator.next();
                        boolean more = stream.stream(next.getValue(), 0, next.getValue().length);
                        return more ? Next.more : Next.stopped;
                    }
                    return Next.eos;
                };
            }

            @Override
            public void close() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public long count() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public boolean isEmpty() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void release() {
                hideABone.release();
            }
        };
    }

    @Override
    public String name() {
        return "RawMemoryIndex";
    }

    @Override
    public IndexRangeId id() {
        return null;
    }

    @Override
    public boolean consume(RawEntryStream stream) throws Exception {
        for (Map.Entry<byte[], byte[]> e : index.entrySet()) {
            byte[] entry = e.getValue();
            if (!stream.stream(entry, 0, entry.length)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean append(RawEntries entries) throws Exception {
        return entries.consume((rawEntry, offset, length) -> {
            int keyLength = UIO.bytesInt(rawEntry);
            byte[] key = new byte[keyLength];
            System.arraycopy(rawEntry, 4, key, 0, keyLength);
            index.compute(key, (k, v) -> {
                byte[] merged;
                if (v == null) {
                    approximateCount.incrementAndGet();
                    merged = rawEntry;
                } else {
                    merged = rawhide.merge(v, rawEntry);
                }
                long timestamp = rawhide.timestamp(merged, 0, merged.length);
                long version = rawhide.version(merged, 0, merged.length);
                updateMaxTimestampAndVersion(timestamp, version);
                return merged;
            });
            return true;
        });
    }

    private void updateMaxTimestampAndVersion(long timestamp, long version) {
        TimestampAndVersion existing = maxTimestampAndVersion.get();
        TimestampAndVersion update = new TimestampAndVersion(timestamp, version);
        while (rawhide.isNewerThan(timestamp, version, existing.maxTimestamp, existing.maxTimestampVersion)) {
            if (maxTimestampAndVersion.compareAndSet(existing, update)) {
                break;
            } else {
                existing = maxTimestampAndVersion.get();
            }
        }
    }

    // you must call release on this reader! Try to only use it as long as you have to!
    @Override
    public ReadIndex acquireReader() throws Exception {
        hideABone.acquire();
        if (disposed.get()) {
            hideABone.release();
            return null;
        }
        return reader;
    }

    @Override
    public void destroy() throws Exception {
        destroy.submit(() -> {
            hideABone.acquire(numBones);
            try {
                disposed.set(true);
                index.clear();
            } finally {
                hideABone.release(numBones);
            }
            return null;
        });
    }

    @Override
    public void closeReadable() throws Exception {
    }

    @Override
    public void closeAppendable(boolean fsync) throws Exception {
    }

    @Override
    public boolean isEmpty() {
        return index.isEmpty();
    }

    @Override
    public long count() {
        return approximateCount.get();
    }

    @Override
    public long sizeInBytes() {
        return 0;
    }

    @Override
    public long keysSizeInBytes() throws IOException {
        return 0;
    }

    @Override
    public long valuesSizeInBytes() throws IOException {
        return 0;
    }

    @Override
    public byte[] minKey() {
        return index.firstKey();
    }

    @Override
    public byte[] maxKey() {
        return index.lastKey();
    }

    @Override
    public TimestampAndVersion maxTimestampAndVersion() {
        return maxTimestampAndVersion.get();
    }

}
