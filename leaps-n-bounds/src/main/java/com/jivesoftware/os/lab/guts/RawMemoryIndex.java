package com.jivesoftware.os.lab.guts;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.lab.api.MergeRawEntry;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.RawAppendableIndex;
import com.jivesoftware.os.lab.guts.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.lab.guts.api.RawEntries;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.io.api.UIO;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class RawMemoryIndex implements RawAppendableIndex, RawConcurrentReadableIndex, RawEntries {

    private final ConcurrentSkipListMap<byte[], byte[]> index = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
    private final AtomicLong approximateCount = new AtomicLong();
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    private final ExecutorService destroy;
    private final MergeRawEntry merger;

    private final int numBones = 1024;
    private final Semaphore hideABone = new Semaphore(numBones, true);
    private final ReadIndex reader;

    public RawMemoryIndex(ExecutorService destroy, MergeRawEntry merger) {
        this.destroy = destroy;
        this.merger = merger;
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
                if (from != null && to == null) {
                    iterator = index.tailMap(from, true).entrySet().iterator();
                } else if (from == null && to != null) {
                    iterator = index.headMap(to, false).entrySet().iterator();

                } else {
                    iterator = index.subMap(from, true, to, false).entrySet().iterator();
                }
                return (stream) -> {
                    if (iterator.hasNext()) {
                        Map.Entry<byte[], byte[]> next = iterator.next();
                        return stream.stream(next.getValue(), 0, next.getValue().length);
                    }
                    return false;
                };
            }

            @Override
            public NextRawEntry rowScan() throws Exception {
                Iterator<Map.Entry<byte[], byte[]>> iterator = index.entrySet().iterator();
                return (stream) -> {
                    if (iterator.hasNext()) {
                        Map.Entry<byte[], byte[]> next = iterator.next();
                        return stream.stream(next.getValue(), 0, next.getValue().length);
                    }
                    return false;
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

            @Override
            public boolean acquire() throws InterruptedException {
                hideABone.acquire();
                if (disposed.get()) {
                    hideABone.release();
                    return false;
                }
                return true;
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
                if (v == null) {
                    approximateCount.incrementAndGet();
                    return rawEntry;
                } else {
                    return merger.merge(v, rawEntry);
                }
            });
            return true;
        });
    }

    // you must call release on this reader! Try to only use it as long have you have to!
    @Override
    public ReadIndex reader() throws Exception {
        hideABone.acquire();
        if (disposed.get()) {
            return null;
        }
        hideABone.release();
        return reader;
    }

    @Override
    public void destroy() throws Exception {
        destroy.submit(() -> {

            hideABone.acquire(numBones);
            disposed.set(true);
            try {
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
    public byte[] minKey() {
        return index.firstKey();
    }

    @Override
    public byte[] maxKey() {
        return index.lastKey();
    }

}
