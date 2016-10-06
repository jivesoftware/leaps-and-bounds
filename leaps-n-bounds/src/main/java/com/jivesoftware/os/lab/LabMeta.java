package com.jivesoftware.os.lab;

import com.jivesoftware.os.jive.utils.collections.baph.ConcurrentBAPHash;
import com.jivesoftware.os.lab.guts.AppendOnlyFile;
import com.jivesoftware.os.lab.guts.ReadOnlyFile;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IPointerReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;

/**
 *
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
                m = compact(metaRoot, activeMeta, m, collisions, m.size());
            }
        }
        meta.set(m);
    }

    public static interface GetMeta<R> {

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
//                    File activeMeta = new File(metaRoot, "active.meta");
//                    Meta compacted = compact(metaRoot, activeMeta, m, count, m.size());
//                    meta.set(compacted);
//                    collisionCount.set(0);
                }
            }
        } finally {
            writeSemaphore.release(Short.MAX_VALUE);
        }
    }

    private Meta compact(File metaRoot, File activeMeta, Meta m, long collisions, long total) throws Exception, IOException {
        LOG.info("Compacting meta:{} because there were collisions:{}/{}", activeMeta, collisions, total);
        File compactingMeta = new File(metaRoot, "compacting.meta");
        FileUtils.deleteQuietly(compactingMeta);
        Meta compacting = new Meta(compactingMeta);
        m.copyTo(compacting);
        m.close();
        compacting.close();
        File backupMeta = new File(metaRoot, "backup.meta");
        FileUtils.deleteQuietly(backupMeta);
        FileUtils.moveFile(activeMeta, backupMeta);
        FileUtils.moveFile(compactingMeta, activeMeta);
        FileUtils.deleteQuietly(backupMeta);
        // TODO filesystem meta fsync?
        Meta active = new Meta(activeMeta);
        active.load();
        return active;
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

    public static interface MetaKeys {

        boolean metaKey(byte[] metaKey);
    }

    static private class Meta {

        private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

        private final File metaFile;
        private volatile ReadOnlyFile readOnlyFile;
        private final AppendOnlyFile appendOnlyFile;
        private final IAppendOnly appender;
        private final ConcurrentBAPHash<byte[]> keyOffsetCache;

        private Meta(File metaFile) throws Exception {
            this.metaFile = metaFile;
            this.appendOnlyFile = new AppendOnlyFile(metaFile);
            this.appender = appendOnlyFile.appender();
            this.readOnlyFile = new ReadOnlyFile(metaFile);
            this.keyOffsetCache = new ConcurrentBAPHash<>(16, true, 1024, (offset) -> {
                IPointerReadable pointerReadable = readOnlyFile.pointerReadable(-1);
                int length = pointerReadable.readInt(offset);
                byte[] bytes = new byte[length];
                pointerReadable.read(offset + 4, bytes, 0, length);
                return bytes;
            });
        }

        public void metaKeys(MetaKeys keys) throws Exception {
            IPointerReadable pointerReadable = readOnlyFile.pointerReadable(-1);
            keyOffsetCache.stream((key, valueOffset) -> {
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
            IPointerReadable readable = readOnlyFile.pointerReadable(-1);
            long o = 0;
            try {
                while (o < readable.length()) {
                    long keyFP = o;
                    int keyLength = readable.readInt(o);
                    o += 4;
                    byte[] key = new byte[keyLength];
                    readable.read(o, key, 0, keyLength);
                    o += keyLength;
                    byte[] got = keyOffsetCache.get(key);
                    if (got != null) {
                        collisions++;
                    }
                    long valueFp = o;
                    int valueLength = readable.readInt(o);
                    o += 4;
                    o += valueLength;

                    if (valueLength > 0) {
                        keyOffsetCache.put(keyFP, key, UIO.longBytes(valueFp));
                    } else {
                        keyOffsetCache.remove(key);
                    }
                }
            } catch (Exception x) {
                LOG.error("Failed to full load labMeta: {} fp:{} length:{}", new Object[]{metaFile, o, metaFile.length()}, x);
            }
            return collisions;
        }

        public void copyTo(Meta to) throws Exception {
            IPointerReadable pointerReadable = readOnlyFile.pointerReadable(-1);
            BolBuffer valueBolBuffer = new BolBuffer();
            keyOffsetCache.stream((key, valueOffset) -> {
                long offset = UIO.bytesLong(valueOffset);
                int length = pointerReadable.readInt(offset);
                pointerReadable.sliceIntoBuffer(offset, length, valueBolBuffer);
                byte[] value = valueBolBuffer.copy();
                if (value.length > 0) {
                    to.append(key, value, false);
                }
                return true;
            });
            to.flush();
        }

        public BolBuffer get(byte[] key, BolBuffer valueBolBuffer) throws Exception {
            byte[] offsetBytes = keyOffsetCache.get(key, 0, key.length);
            if (offsetBytes == null) {
                return null;
            }
            long offset = -1;
            int length = -1;
            try {
                offset = UIO.bytesLong(offsetBytes);
                IPointerReadable pointerReadable = readOnlyFile.pointerReadable(-1);
                length = pointerReadable.readInt(offset);
                pointerReadable.sliceIntoBuffer(offset + 4, length, valueBolBuffer);
                return valueBolBuffer;
            } catch (Exception x) {
                LOG.error("Failed to get({}) offset:{} length:{}", Arrays.toString(key), offset, length);
                throw x;
            }
        }

        public boolean append(byte[] key, byte[] value, boolean flush) throws Exception {

            long keyPointer = appender.getFilePointer();
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
                keyOffsetCache.remove(key);
                return true;
            } else {
                byte[] had = keyOffsetCache.get(key);
                keyOffsetCache.put(keyPointer, key, UIO.longBytes(valuePointer));
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
            return keyOffsetCache.size();
        }
    }
}
