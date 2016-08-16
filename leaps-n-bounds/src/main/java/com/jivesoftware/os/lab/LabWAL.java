package com.jivesoftware.os.lab;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.collections.bah.BAHEqualer;
import com.jivesoftware.os.jive.utils.collections.bah.BAHMapState;
import com.jivesoftware.os.jive.utils.collections.bah.BAHash;
import com.jivesoftware.os.jive.utils.collections.bah.BAHasher;
import com.jivesoftware.os.lab.api.FormatTransformerProvider;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.guts.IndexFile;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    private static final byte FSYNC_ISOLATION = 2;

    private static final int[] MAGIC = {
        351126232,
        759984878,
        266850631
    };

    private final File walRoot;
    private final long maxWALSizeInBytes;
    private final long maxEntriesPerWAL;
    private final long maxEntrySizeInBytes;
    private final List<ActiveWAL> oldWALs = Lists.newArrayList();
    private final AtomicReference<ActiveWAL> activeWAL = new AtomicReference<>();
    private final AtomicLong walIdProvider = new AtomicLong();

    public LabWAL(File walRoot,
        long maxWALSizeInBytes,
        long maxEntriesPerWAL,
        long maxEntrySizeInBytes) throws IOException {

        this.walRoot = walRoot;
        this.maxWALSizeInBytes = maxWALSizeInBytes;
        this.maxEntriesPerWAL = maxEntriesPerWAL;
        this.maxEntrySizeInBytes = maxEntrySizeInBytes;

    }

    public void open(LABEnvironment environment) throws Exception {

        File[] walFiles = walRoot.listFiles();
        if (walFiles == null) {
            return;
        }

        Arrays.sort(walFiles, (wal1, wal2) -> Long.compare(Long.parseLong(wal1.getName()), Long.parseLong(wal2.getName())));

        long maxWALId = 0;
        for (int i = 0; i < walFiles.length; i++) {
            File walFile = walFiles[i];
            try {
                maxWALId = Math.max(maxWALId, Long.parseLong(walFile.getName()));
            } catch (NumberFormatException nfe) {
                LOG.error("Encoudered an unexpected file name:" + walFile + " in " + walRoot);
                walFiles[i] = null;
            }
        }
        walIdProvider.set(maxWALId);

        List<IndexFile> deleteableIndexFiles = Lists.newArrayList();
        Map<String, ListMultimap<Long, byte[]>> allEntries = Maps.newHashMap();
        Map<String, ListMultimap<Long, byte[]>> appendableEntries = Maps.newHashMap();
        BAHash<ValueIndex> valueIndexes = new BAHash<>(new BAHMapState<>(10, true, BAHMapState.NIL), BAHasher.SINGLETON, BAHEqualer.SINGLETON);
        for (File walFile : walFiles) {
            if (walFile == null) {
                continue;
            }

            IndexFile indexFile = null;
            try {
                indexFile = new IndexFile(walFile, "rw", false);
                IReadable reader = indexFile.reader(null, indexFile.length(), true);
                reader.seek(0);

                int rowType = reader.read();
                if (rowType < 0 || rowType > 127) {
                    throw new IllegalStateException("expected a row type greater than -1 and less than 128 but encountered " + rowType);
                }
                int magic = reader.readInt();
                if (magic != MAGIC[rowType]) {
                    throw new IllegalStateException("expected a magic " + MAGIC[rowType] + " but encountered " + magic);
                }
                int valueIndexIdLength = reader.readInt();
                if (valueIndexIdLength >= maxEntrySizeInBytes) {
                    throw new IllegalStateException("valueIndexId length corruption" + valueIndexIdLength + ">=" + maxEntrySizeInBytes);
                }

                byte[] valueIndexId = new byte[valueIndexIdLength];
                reader.read(valueIndexId);

                String valueIndexKey = new String(valueIndexId, StandardCharsets.UTF_8);
                long version = reader.readLong();

                if (rowType == ENTRY) {
                    if (!appendableEntries.isEmpty()) {
                        for (Map.Entry<String, ListMultimap<Long, byte[]>> entry : appendableEntries.entrySet()) {
                            String valueIndexIdAsString = entry.getKey();
                            ValueIndexConfig valueIndexConfig = environment.valueIndexConfig(valueIndexIdAsString.getBytes(StandardCharsets.UTF_8));
                            FormatTransformerProvider formatTransformerProvider = environment.formatTransformerProvider(valueIndexConfig.formatTransformerProviderName);
                            Rawhide rawhide = environment.rawhide(valueIndexConfig.rawhideName);
                            RawEntryFormat rawEntryFormat = environment.rawEntryFormat(valueIndexConfig.rawEntryFormatName);


                            ListMultimap<Long, byte[]> versionedAppends = entry.getValue();
                            ValueIndex appendToValueIndex = openValueIndex(environment, valueIndexIdAsString.getBytes(StandardCharsets.UTF_8), valueIndexes);
                            for (Long appendVersion : versionedAppends.keySet()) {
                                appendToValueIndex.append((ValueStream stream) -> {
                                    for (byte[] e : versionedAppends.get(appendVersion)) {
                                        
                                        //LABRawhide.SINGLETON.streamRawEntry(-1, FormatTransformer.NO_OP, FormatTransformer.NO_OP, e, magic, stream, true);
                                    }
                                    return true;
                                }, true);
                            }
                            appendToValueIndex.commit(true, true);
                        }
                        appendableEntries.clear();
                    }

                    int entryLength = reader.readInt();
                    if (entryLength >= maxEntrySizeInBytes) {
                        throw new IllegalStateException("entryLength length corruption" + entryLength + ">=" + maxEntrySizeInBytes);
                    }

                    byte[] entry = new byte[entryLength];
                    reader.read(entry);

                    ListMultimap<Long, byte[]> valueIndexVersionedEntries = allEntries.computeIfAbsent(valueIndexKey, (k) -> ArrayListMultimap.create());
                    valueIndexVersionedEntries.put(version, entry);

                } else if (rowType == BATCH_ISOLATION) {
                    ListMultimap<Long, byte[]> valueIndexVersionedEntries = allEntries.get(valueIndexKey);
                    if (valueIndexVersionedEntries != null) {
                        ListMultimap<Long, byte[]> valueIndexAppendableEntries = appendableEntries.computeIfAbsent(valueIndexKey, (k) -> ArrayListMultimap.create());
                        for (byte[] entry : valueIndexVersionedEntries.get(version)) {
                            valueIndexAppendableEntries.put(version, entry);
                        }
                    }

                } else if (rowType == FSYNC_ISOLATION) {
                    long fsyncVersion = reader.readLong();
                    ListMultimap<Long, byte[]> valueIndexVersionedEntries = appendableEntries.get(valueIndexKey);
                    if (valueIndexVersionedEntries != null) {
                        valueIndexVersionedEntries.remove(version, magic);
                    }
                }

                int length = reader.readInt();
                if (length >= maxEntrySizeInBytes) {
                    throw new IllegalStateException("expected row length less than " + maxEntrySizeInBytes + " but encountered " + length);
                }
                deleteableIndexFiles.add(indexFile);
            } finally {
                if (indexFile != null) {
                    indexFile.close();
                }
            }

        }

        if (!appendableEntries.isEmpty()) {

        }

        if (!allEntries.isEmpty()) {
            // Incomplete writes baby
        }

        if (!deleteableIndexFiles.isEmpty()) {
            for (IndexFile deletableIndexFile : deleteableIndexFiles) {
                deletableIndexFile.delete();
            }
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

    public void close(LABEnvironment environment) throws IOException {
        while (true) {
            ActiveWAL wal = activeWAL.get();
            if (wal != null) {
                synchronized (activeWAL) {
                    //wal.close(environment);
                    //wal.remove();
                    activeWAL.set(null);
                }
            }
            for (ActiveWAL oldWAL : oldWALs) {
                //oldWAL.close(environment);
                //oldWAL.remove();
            }
        }
    }

    public void append(byte[] valueIndexId, long appendVersion, byte[] entry) throws Exception {
        activeWAL().append(valueIndexId, appendVersion, entry);
    }

    public void flush(byte[] valueIndexId, long appendVersion, boolean fsync) throws Exception {
        ActiveWAL wal = activeWAL();

        long fsyncedVersion = wal.flushed(valueIndexId, appendVersion, fsync);

        if (wal.entryCount.get() > maxEntriesPerWAL || wal.sizeInBytes.get() > maxWALSizeInBytes) {
            synchronized (activeWAL) {
                wal = allocateNewWAL();
                ActiveWAL oldWAL = activeWAL.getAndSet(wal);
                oldWALs.add(oldWAL);
            }
        }

        if (!oldWALs.isEmpty()) {
            List<ActiveWAL> removeable = Lists.newArrayList();
            for (ActiveWAL oldWAL : oldWALs) {
                if (oldWAL.removeable(valueIndexId, fsyncedVersion)) {
                    removeable.add(wal);
                }
            }

            if (!removeable.isEmpty()) {
                synchronized (activeWAL) {
                    oldWALs.removeAll(removeable);
                }
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
        IndexFile walFile = new IndexFile(file, "rw", false);
        wal = new ActiveWAL(walFile);
        return wal;
    }

    private static final class ActiveWAL {

        private final IndexFile wal;
        private final IAppendOnly appendOnly;
        private final BAHash<AppendAndFsyncVersion> flushAndFsyncVersions;
        private final AtomicLong entryCount = new AtomicLong();
        private final AtomicLong sizeInBytes = new AtomicLong();
        private final AtomicLong fsyncVersion = new AtomicLong();
        private final Object oneWriteAtTimeLock = new Object();

        public ActiveWAL(IndexFile wal) throws Exception {
            this.wal = wal;
            this.flushAndFsyncVersions = new BAHash<>(new BAHMapState<>(10, true, BAHMapState.NIL), BAHasher.SINGLETON, BAHEqualer.SINGLETON);
            this.appendOnly = wal.appender();
        }

        private void append(byte type, byte[] valueIndexId, long version) throws IOException {
            appendOnly.appendByte(type);
            appendOnly.appendInt(MAGIC[type]);
            UIO.writeByteArray(appendOnly, valueIndexId, "valueIndexId");
            appendOnly.appendLong(version);
        }

        public void append(byte[] valueIndexId, long appendVersion, byte[] entry) throws Exception {
            long fsyncVersion;
            synchronized (oneWriteAtTimeLock) {
                fsyncVersion = this.fsyncVersion.get();
                append(ENTRY, valueIndexId, appendVersion);
                UIO.writeByteArray(appendOnly, entry, "entry");
                flushAndFsyncVersions.put(entry, new AppendAndFsyncVersion(appendVersion, fsyncVersion));
            }
        }

        public long flushed(byte[] valueIndexId, long appendVersion, boolean fsync) throws Exception {

            append(BATCH_ISOLATION, valueIndexId, appendVersion);

            long fsyncedVersion;
            synchronized (oneWriteAtTimeLock) {
                AppendAndFsyncVersion appendAndFsyncVersion = flushAndFsyncVersions.get(valueIndexId, 0, valueIndexId.length);
                fsyncedVersion = fsyncVersion.get();
                if (appendAndFsyncVersion != null && appendAndFsyncVersion.fsyncVersion >= fsyncedVersion) {
                    if (fsync) {
                        append(FSYNC_ISOLATION, valueIndexId, appendVersion);
                        appendOnly.appendLong(fsyncedVersion);
                        wal.flush(fsync);
                        fsyncVersion.incrementAndGet();
                    }
                    flushAndFsyncVersions.remove(valueIndexId, 0, valueIndexId.length);
                }
            }
            return fsyncedVersion;
        }

        public boolean removeable(byte[] valueIndexId, long fsyncedVersion) {
            synchronized (oneWriteAtTimeLock) {
                if (flushAndFsyncVersions.size() == 0) {
                    return true;
                }
                if (valueIndexId != null) {
                    AppendAndFsyncVersion appendAndFsyncVersion = flushAndFsyncVersions.get(valueIndexId, 0, valueIndexId.length);
                    if (appendAndFsyncVersion != null && appendAndFsyncVersion.fsyncVersion >= fsyncedVersion) {
                        flushAndFsyncVersions.remove(valueIndexId, 0, valueIndexId.length);
                    }
                }
            }
            return false;
        }

        public void close() throws IOException {
            wal.close();
        }

        static class AppendAndFsyncVersion {

            final long appendVersion;
            final long fsyncVersion;

            public AppendAndFsyncVersion(long appendVersion, long fsyncVersion) {
                this.appendVersion = appendVersion;
                this.fsyncVersion = fsyncVersion;
            }
        }
    }
}
