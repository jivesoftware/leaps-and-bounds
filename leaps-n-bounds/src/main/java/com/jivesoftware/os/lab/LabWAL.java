package com.jivesoftware.os.lab;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.collections.bah.BAHEqualer;
import com.jivesoftware.os.jive.utils.collections.bah.BAHMapState;
import com.jivesoftware.os.jive.utils.collections.bah.BAHash;
import com.jivesoftware.os.jive.utils.collections.bah.BAHasher;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.FormatTransformerProvider;
import com.jivesoftware.os.lab.api.LABFailedToInitializeWALException;
import com.jivesoftware.os.lab.api.LABIndexClosedException;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.guts.IndexFile;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.io.AppendableHeap;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author jonathan.colt
 */
public class LabWAL {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final byte ENTRY = 0;
    private static final byte BATCH_ISOLATION = 1;
    private static final byte COMMIT_ISOLATION = 2;
    private static final int[] MAGIC = new int[3];

    static {
        MAGIC[ENTRY] = 351126232;
        MAGIC[BATCH_ISOLATION] = 759984878;
        MAGIC[COMMIT_ISOLATION] = 266850631;
    }

    private final List<ActiveWAL> oldWALs = Lists.newCopyOnWriteArrayList();
    private final AtomicReference<ActiveWAL> activeWAL = new AtomicReference<>();
    private final AtomicLong walIdProvider = new AtomicLong();
    private final Semaphore semaphore = new Semaphore(Short.MAX_VALUE, true);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AppendableHeap appendableHeap = new AppendableHeap(8192);

    private final File walRoot;
    private final long maxWALSizeInBytes;
    private final long maxEntriesPerWAL;
    private final long maxEntrySizeInBytes;
    private final long maxValueIndexHeapPressureOverride;

    public LabWAL(File walRoot,
        long maxWALSizeInBytes,
        long maxEntriesPerWAL,
        long maxEntrySizeInBytes,
        long maxValueIndexHeapPressureOverride) throws IOException {

        this.walRoot = walRoot;
        this.maxWALSizeInBytes = maxWALSizeInBytes;
        this.maxEntriesPerWAL = maxEntriesPerWAL;
        this.maxEntrySizeInBytes = maxEntrySizeInBytes;
        this.maxValueIndexHeapPressureOverride = maxValueIndexHeapPressureOverride;
    }

    public int oldWALCount() {
        return oldWALs.size();
    }

    public long activeWALId() {
        return walIdProvider.get();
    }

    public void open(LABEnvironment environment) throws Exception {

        File[] walFiles = walRoot.listFiles();
        if (walFiles == null) {
            return;
        }

        long maxWALId = 0;
        List<File> listWALFiles = Lists.newArrayList();
        for (File walFile : walFiles) {
            try {
                maxWALId = Math.max(maxWALId, Long.parseLong(walFile.getName()));
                listWALFiles.add(walFile);
            } catch (NumberFormatException nfe) {
                LOG.error("Encoudered an unexpected file name:" + walFile + " in " + walRoot);
            }
        }
        walIdProvider.set(maxWALId);

        Collections.sort(listWALFiles, (wal1, wal2) -> Long.compare(Long.parseLong(wal1.getName()), Long.parseLong(wal2.getName())));

        List<IndexFile> deleteableIndexFiles = Lists.newArrayList();
        Map<String, ListMultimap<Long, byte[]>> allEntries = Maps.newHashMap();

        BAHash<ValueIndex> valueIndexes = new BAHash<>(new BAHMapState<>(10, true, BAHMapState.NIL), BAHasher.SINGLETON, BAHEqualer.SINGLETON);
        BolBuffer rawEntryBuffer = new BolBuffer();
        for (File walFile : listWALFiles) {

            IndexFile indexFile = null;
            try {
                indexFile = new IndexFile(walFile, "r");
                deleteableIndexFiles.add(indexFile);

                IReadable reader = indexFile.reader(null, indexFile.length());
                reader.seek(0);
                try {
                    while (true) {
                        int rowType = reader.read();
                        if (rowType == -1) {
                            break; //EOF
                        }
                        if (rowType > 3) {
                            throw new CorruptionDetectedException("expected a row type greater than -1 and less than 128 but encountered " + rowType);
                        }
                        int magic = reader.readInt();
                        if (magic != MAGIC[rowType]) {
                            throw new CorruptionDetectedException("expected a magic " + MAGIC[rowType] + " but encountered " + magic);
                        }
                        int valueIndexIdLength = reader.readInt();
                        if (valueIndexIdLength >= maxEntrySizeInBytes) {
                            throw new CorruptionDetectedException("valueIndexId length corruption" + valueIndexIdLength + ">=" + maxEntrySizeInBytes);
                        }
                        
                        byte[] valueIndexId = new byte[valueIndexIdLength];
                        reader.read(valueIndexId);

                        String valueIndexKey = new String(valueIndexId, StandardCharsets.UTF_8);
                        long appendVersion = reader.readLong();

                        if (rowType == ENTRY) {
                            int entryLength = reader.readInt();
                            if (entryLength >= maxEntrySizeInBytes) {
                                throw new CorruptionDetectedException("entryLength length corruption" + entryLength + ">=" + maxEntrySizeInBytes);
                            }

                            byte[] entry = new byte[entryLength];
                            reader.read(entry);

                            ListMultimap<Long, byte[]> valueIndexVersionedEntries = allEntries.computeIfAbsent(valueIndexKey,
                                (k) -> ArrayListMultimap.create());
                            valueIndexVersionedEntries.put(appendVersion, entry);

                        } else if (rowType == BATCH_ISOLATION) {
                            ListMultimap<Long, byte[]> valueIndexVersionedEntries = allEntries.get(valueIndexKey);
                            if (valueIndexVersionedEntries != null) {
                                ValueIndexConfig valueIndexConfig = environment.valueIndexConfig(valueIndexKey.getBytes(StandardCharsets.UTF_8));
                                FormatTransformerProvider formatTransformerProvider = environment.formatTransformerProvider(
                                    valueIndexConfig.formatTransformerProviderName);
                                Rawhide rawhide = environment.rawhide(valueIndexConfig.rawhideName);
                                RawEntryFormat rawEntryFormat = environment.rawEntryFormat(valueIndexConfig.rawEntryFormatName);
                                LAB appendToValueIndex = (LAB) openValueIndex(environment, valueIndexId, valueIndexes);

                                FormatTransformer readKey = formatTransformerProvider.read(rawEntryFormat.getKeyFormat());
                                FormatTransformer readValue = formatTransformerProvider.read(rawEntryFormat.getValueFormat());

                                appendToValueIndex.append((stream) -> {

                                    ValueStream valueStream = (index, key, timestamp, tombstoned, version, payload) -> {
                                        return stream.stream(index, IndexUtil.toByteArray(key), timestamp, tombstoned, version, IndexUtil.toByteArray(payload));
                                    };

                                    for (byte[] entry : valueIndexVersionedEntries.get(appendVersion)) {

                                        if (!rawhide.streamRawEntry(-1, readKey, readValue, ByteBuffer.wrap(entry), valueStream, true)) {
                                            return false;
                                        }
                                    }
                                    return true;
                                }, true, maxValueIndexHeapPressureOverride, rawEntryBuffer);

                                valueIndexVersionedEntries.removeAll(appendVersion);
                            }
                        }
                    }
                } catch (CorruptionDetectedException | EOFException x) {
                    LOG.warn("Corruption detected at fp:{} length:{} for file:{} cause:{}", reader.getFilePointer(), reader.length(), walFile, x.getClass());
                } catch (Exception x) {
                    LOG.error("Encountered an issue that requires intervention at fp:{} length:{} for file:{}",
                        new Object[]{reader.getFilePointer(), reader.length(), walFile}, x);
                    throw new LABFailedToInitializeWALException("Encountered an issue in " + walFile + " please help.", x);
                }
            } finally {
                if (indexFile != null) {
                    indexFile.close();
                }
            }

        }

        try {
            valueIndexes.stream((byte[] key, ValueIndex value) -> {
                value.close(true, true);
                return true;
            });
        } catch (Exception x) {
            throw new LABFailedToInitializeWALException("Encountered an issue while commiting and closing. Please help.", x);
        }

        for (IndexFile deletableIndexFile : deleteableIndexFiles) {
            try {
                deletableIndexFile.delete();
            } catch (Exception x) {
                throw new LABFailedToInitializeWALException(
                    "Encountered an issue while deleting WAL:" + deletableIndexFile.getFileName() + ". Please help.", x);
            }
        }
    }

    static class CorruptionDetectedException extends Exception {

        CorruptionDetectedException(String cause) {
            super(cause);
        }

    }

    private ValueIndex openValueIndex(LABEnvironment environment, byte[] valueIndexId, BAHash<ValueIndex> valueIndexes) throws Exception {
        ValueIndex valueIndex = valueIndexes.get(valueIndexId, 0, valueIndexId.length);
        if (valueIndex == null) {
            ValueIndexConfig valueIndexConfig = environment.valueIndexConfig(valueIndexId);
            valueIndex = environment.open(valueIndexConfig);
            valueIndexes.put(valueIndexId, valueIndex);
        }
        return valueIndex;
    }

    public void close(LABEnvironment environment) throws IOException, InterruptedException {
        semaphore.acquire(Short.MAX_VALUE);
        try {
            if (closed.compareAndSet(false, true)) {
                ActiveWAL wal = activeWAL.get();
                if (wal != null) {
                    wal.close();
                    activeWAL.set(null);
                }
                for (ActiveWAL oldWAL : oldWALs) {
                    oldWAL.close();
                }
                oldWALs.clear();
            }
        } finally {
            semaphore.release(Short.MAX_VALUE);
        }
    }

    public void append(byte[] valueIndexId, long appendVersion, BolBuffer entry) throws Exception {
        semaphore.acquire();
        try {
            if (closed.get()) {
                throw new LABIndexClosedException("Trying to write to a Lab WAL that has been closed.");
            }
            activeWAL().append(appendableHeap, valueIndexId, appendVersion, entry);
        } finally {
            semaphore.release();
        }
    }

    public void flush(byte[] valueIndexId, long appendVersion, boolean fsync) throws Exception {
        boolean needToAllocateNewWAL = false;
        semaphore.acquire();
        try {
            if (closed.get()) {
                throw new LABIndexClosedException("Trying to write to a Lab WAL that has been closed.");
            }
            ActiveWAL wal = activeWAL();
            wal.flushed(appendableHeap, valueIndexId, appendVersion, fsync);
            if (wal.entryCount.get() > maxEntriesPerWAL || wal.sizeInBytes.get() > maxWALSizeInBytes) {
                needToAllocateNewWAL = true;
            }
        } finally {
            semaphore.release();
        }

        if (needToAllocateNewWAL) {
            semaphore.acquire(Short.MAX_VALUE);
            try {
                if (closed.get()) {
                    throw new LABIndexClosedException("Trying to write to a Lab WAL that has been closed.");
                }
                ActiveWAL wal = activeWAL();
                if (wal.entryCount.get() > maxEntriesPerWAL || wal.sizeInBytes.get() > maxWALSizeInBytes) {
                    wal = allocateNewWAL();
                    ActiveWAL oldWAL = activeWAL.getAndSet(wal);
                    oldWAL.close();
                    oldWALs.add(oldWAL);
                }
            } finally {
                semaphore.release(Short.MAX_VALUE);
            }
        }
    }

    public void commit(byte[] valueIndexId, long appendVersion, boolean fsync) throws Exception {
        List<ActiveWAL> removeable = null;
        semaphore.acquire();
        try {
            if (closed.get()) {
                throw new LABIndexClosedException("Trying to write to a Lab WAL that has been closed.");
            }
            ActiveWAL wal = activeWAL();
            wal.commit(valueIndexId, appendVersion, fsync);
            if (!oldWALs.isEmpty()) {
                removeable = Lists.newArrayList();
                for (ActiveWAL oldWAL : oldWALs) {
                    if (oldWAL.commit(valueIndexId, appendVersion, fsync)) {
                        removeable.add(oldWAL);
                    }
                }
            }
        } finally {
            semaphore.release();
        }

        if (removeable != null && !removeable.isEmpty()) {
            semaphore.acquire(Short.MAX_VALUE);
            try {
                if (closed.get()) {
                    throw new LABIndexClosedException("Trying to write to a Lab WAL that has been closed.");
                }
                for (ActiveWAL remove : removeable) {
                    remove.delete();
                }
                oldWALs.removeAll(removeable);
            } finally {
                semaphore.release(Short.MAX_VALUE);
            }
        }
    }

    private ActiveWAL activeWAL() throws Exception {
        ActiveWAL wal = activeWAL.get();
        if (wal == null) {
            synchronized (activeWAL) {
                wal = activeWAL.get();
                if (wal == null) {
                    wal = allocateNewWAL();
                    activeWAL.set(wal);
                }
            }
        }
        return wal;
    }

    private ActiveWAL allocateNewWAL() throws Exception {
        ActiveWAL wal;
        File file = new File(walRoot, String.valueOf(walIdProvider.incrementAndGet()));
        file.getParentFile().mkdirs();
        IndexFile walFile = new IndexFile(file, "rw");
        wal = new ActiveWAL(walFile);
        return wal;
    }

    private static final class ActiveWAL {

        private final IndexFile wal;
        private final IAppendOnly appendOnly;
        private final BAHash<Long> appendVersions;
        private final AtomicLong entryCount = new AtomicLong();
        private final AtomicLong sizeInBytes = new AtomicLong();
        private final Object oneWriteAtTimeLock = new Object();

        public ActiveWAL(IndexFile wal) throws Exception {
            this.wal = wal;
            this.appendVersions = new BAHash<>(new BAHMapState<>(10, true, BAHMapState.NIL), BAHasher.SINGLETON, BAHEqualer.SINGLETON);
            this.appendOnly = wal.appender();
        }

        public void append(AppendableHeap appendableHeap, byte[] valueIndexId, long appendVersion, BolBuffer entry) throws Exception {
            entryCount.incrementAndGet();
            sizeInBytes.addAndGet(1 + 4 + 4 + valueIndexId.length + 8 + entry.length);
            synchronized (oneWriteAtTimeLock) {
                append(appendableHeap, ENTRY, valueIndexId, appendVersion);
                UIO.writeByteArray(appendableHeap, entry.bytes, 0, entry.length, "entry");
            }
        }

        public void flushed(AppendableHeap appendableHeap, byte[] valueIndexId, long appendVersion, boolean fsync) throws Exception {
            sizeInBytes.addAndGet(1 + 4 + 4 + valueIndexId.length + 8);
            synchronized (oneWriteAtTimeLock) {
                append(appendableHeap, BATCH_ISOLATION, valueIndexId, appendVersion);

                appendOnly.append(appendableHeap.leakBytes(), 0, (int) appendableHeap.length());
                appendOnly.flush(fsync);

                appendableHeap.reset();
                appendVersions.put(valueIndexId, appendVersion);
            }
        }

        private void append(AppendableHeap appendableHeap, byte type, byte[] valueIndexId, long appendVersion) throws IOException {
            appendableHeap.appendByte(type);
            appendableHeap.appendInt(MAGIC[type]);
            UIO.writeByteArray(appendableHeap, valueIndexId, "valueIndexId");
            appendableHeap.appendLong(appendVersion);
        }

        public boolean commit(byte[] valueIndexId, long appendVersion, boolean fsync) throws Exception {
            synchronized (oneWriteAtTimeLock) {
                Long lastAppendVersion = appendVersions.get(valueIndexId, 0, valueIndexId.length);
                if (lastAppendVersion != null && lastAppendVersion < appendVersion) {
                    appendVersions.remove(valueIndexId, 0, valueIndexId.length);
                }
                return appendVersions.size() == 0;
            }
        }

        public void close() throws IOException {
            wal.close();
        }

        public void delete() throws IOException {
            wal.close();
            wal.delete();
        }

    }
}
