package com.jivesoftware.os.lab;

import com.jivesoftware.os.jive.utils.collections.bah.ConcurrentBAHash;
import com.jivesoftware.os.lab.guts.AppendOnlyFile;
import com.jivesoftware.os.lab.guts.ReadOnlyFile;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.PointerReadableByteBufferFile;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;

/**
 * @author jonathan.colt
 */
public class LabMeta {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final File metaRoot;
    private final Semaphore writeSemaphore = new Semaphore(Short.MAX_VALUE);
    private final AtomicReference<Meta> meta = new AtomicReference<>();
    private final AtomicLong collisionCount = new AtomicLong();

    public LabMeta(File metaRoot) throws Exception {
        this.metaRoot = metaRoot;
        File activeMeta = new File(metaRoot, "active.meta");

        if (!activeMeta.exists()) {
            File backupMeta = new File(metaRoot, "backup.meta");
            if (backupMeta.exists()) {
                FileUtils.moveFile(backupMeta, activeMeta);
            } else {
                activeMeta.getParentFile().mkdirs();
            }
        }
        Meta m = new Meta(activeMeta);
        if (activeMeta.exists()) {
            int collisions = m.load();
            if (collisions > m.size() / 2) {
                m = compact(m, collisions, m.size());
            }
        }
        meta.set(m);
    }

    public interface GetMeta<R> {

        R metaValue(BolBuffer metaValue) throws Exception;
    }

    public <R> R get(byte[] key, GetMeta<R> getMeta) throws Exception {
        writeSemaphore.acquire();
        try {
            Meta m = meta.get();
            BolBuffer metaValue = m.get(key, new BolBuffer());
            return getMeta.metaValue(metaValue);
        } finally {
            writeSemaphore.release();
        }
    }

    public void append(byte[] key, byte[] value, boolean flush) throws Exception {
        writeSemaphore.acquire(Short.MAX_VALUE);
        try {
            Meta m = meta.get();
            if (m.append(key, value, flush)) {
                long count = collisionCount.incrementAndGet();
                if (count > m.size() / 2) {
                    Meta compacted = compact(m, count, m.size());
                    meta.set(compacted);
                    collisionCount.set(0);
                }
            }
        } finally {
            writeSemaphore.release(Short.MAX_VALUE);
        }
    }

    private Meta compact(Meta active, long collisions, long total) throws Exception {
        active.flush();
        File activeMeta = new File(metaRoot, "active.meta");
        File compactingMeta = new File(metaRoot, "compacting.meta");

        LOG.info("Compacting meta:{} because there were collisions:{} / {}", activeMeta, collisions, total);
        if (compactingMeta.exists()) {
            FileUtils.forceDelete(compactingMeta);
        }
        Meta compacting = new Meta(compactingMeta);
        active.copyTo(compacting);
        compacting.flush();
        active.close();
        compacting.close();
        File backupMeta = new File(metaRoot, "backup.meta");
        if (backupMeta.exists()) {
            FileUtils.forceDelete(backupMeta);
        }
        FileUtils.moveFile(activeMeta, backupMeta);
        FileUtils.moveFile(compactingMeta, activeMeta);
        if (backupMeta.exists()) {
            FileUtils.forceDelete(backupMeta);
        }
        // TODO filesystem meta fsync?
        Meta compacted = new Meta(activeMeta);
        compacted.load();
        return compacted;
    }

    public void close() throws Exception {
        writeSemaphore.acquire(Short.MAX_VALUE);
        try {
            meta.get().close();
        } finally {
            writeSemaphore.release(Short.MAX_VALUE);
        }
    }

    public void metaKeys(MetaKeys metaKeys) throws Exception {
        writeSemaphore.acquire();
        try {
            meta.get().metaKeys(metaKeys);
        } finally {
            writeSemaphore.release();
        }
    }

    public interface MetaKeys {

        boolean metaKey(byte[] metaKey);
    }

    static private class Meta {

        private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

        private final File metaFile;
        private volatile ReadOnlyFile readOnlyFile;
        private final AppendOnlyFile appendOnlyFile;
        private final IAppendOnly appender;
        private final ConcurrentBAHash<byte[]> offsetKeyOffsetValueCache;

        private Meta(File metaFile) throws Exception {
            this.metaFile = metaFile;
            this.appendOnlyFile = new AppendOnlyFile(metaFile);
            this.appender = appendOnlyFile.appender();
            this.readOnlyFile = new ReadOnlyFile(metaFile);
            this.offsetKeyOffsetValueCache = new ConcurrentBAHash<>(16, true, 1024);
        }

        public void metaKeys(MetaKeys keys) throws Exception {
            PointerReadableByteBufferFile pointerReadable = readOnlyFile.pointerReadable(-1);
            offsetKeyOffsetValueCache.stream((key, valueOffset) -> {
                long offset = UIO.bytesLong(valueOffset);
                int length = pointerReadable.readInt(offset);
                if (length > 0) {
                    return keys.metaKey(key);
                }
                return true;
            });
        }

        public void close() throws Exception {
            readOnlyFile.close();
            appendOnlyFile.close();
            appender.close();
        }

        public int load() throws Exception {
            int collisions = 0;
            PointerReadableByteBufferFile readable = readOnlyFile.pointerReadable(-1);
            long o = 0;
            try {
                while (o < readable.length()) {
                    int keyLength = readable.readInt(o);
                    o += 4;
                    byte[] key = new byte[keyLength];
                    readable.read(o, key, 0, keyLength);
                    o += keyLength;
                    byte[] got = offsetKeyOffsetValueCache.get(key);
                    if (got != null) {
                        collisions++;
                    }
                    long valueFp = o;
                    int valueLength = readable.readInt(o);
                    o += 4;
                    o += valueLength;

                    if (valueLength > 0) {
                        offsetKeyOffsetValueCache.put(key, UIO.longBytes(valueFp));
                    } else {
                        offsetKeyOffsetValueCache.remove(key);
                    }
                }
            } catch (Exception x) {
                LOG.error("Failed to full load labMeta: {} fp:{} length:{}", new Object[] { metaFile, o, metaFile.length() }, x);
            }
            return collisions;
        }

        public void copyTo(Meta to) throws Exception {
            PointerReadableByteBufferFile pointerReadable = readOnlyFile.pointerReadable(-1);
            offsetKeyOffsetValueCache.stream((key, valueOffset) -> {
                long offset = UIO.bytesLong(valueOffset);
                int length = pointerReadable.readInt(offset);
                if (length > 0) {
                    byte[] value = new byte[length];
                    pointerReadable.read(offset + 4, value, 0, length);
                    to.append(key, value, false);
                }
                return true;
            });
            to.flush();
        }

        public BolBuffer get(byte[] key, BolBuffer valueBolBuffer) throws Exception {
            byte[] offsetBytes = offsetKeyOffsetValueCache.get(key, 0, key.length);
            if (offsetBytes == null) {
                return null;
            }
            long offset = -1;
            int length = -1;
            try {
                offset = UIO.bytesLong(offsetBytes);
                PointerReadableByteBufferFile pointerReadable = readOnlyFile.pointerReadable(-1);
                length = pointerReadable.readInt(offset);
                pointerReadable.sliceIntoBuffer(offset + 4, length, valueBolBuffer);
                return valueBolBuffer;
            } catch (Exception x) {
                LOG.error("Failed to get({}) offset:{} length:{}", Arrays.toString(key), offset, length);
                throw x;
            }
        }

        public boolean append(byte[] key, byte[] value, boolean flush) throws Exception {

            appender.appendInt(key.length);
            appender.append(key, 0, key.length);
            long valuePointer = appender.getFilePointer();
            appender.appendInt(value.length);
            appender.append(value, 0, value.length);

            if (flush) {
                appender.flush(flush);

                ReadOnlyFile current = readOnlyFile;
                readOnlyFile = new ReadOnlyFile(metaFile);
                current.close();
            }

            if (value.length == 0) {
                offsetKeyOffsetValueCache.remove(key);
                return true;
            } else {
                byte[] had = offsetKeyOffsetValueCache.get(key);
                offsetKeyOffsetValueCache.put(key, UIO.longBytes(valuePointer));
                return had != null;
            }
        }

        public void flush() throws Exception {
            appender.flush(true);

            ReadOnlyFile current = readOnlyFile;
            readOnlyFile = new ReadOnlyFile(metaFile);
            current.close();
        }

        private int size() {
            return offsetKeyOffsetValueCache.size();
        }
    }
}
