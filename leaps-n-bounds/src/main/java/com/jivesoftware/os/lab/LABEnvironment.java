package com.jivesoftware.os.lab;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.FormatTransformerProvider;
import com.jivesoftware.os.lab.api.MemoryRawEntryFormat;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.api.rawhide.KeyValueRawhide;
import com.jivesoftware.os.lab.api.rawhide.LABRawhide;
import com.jivesoftware.os.lab.api.rawhide.Rawhide;
import com.jivesoftware.os.lab.guts.AppendOnlyFile;
import com.jivesoftware.os.lab.guts.LABCSLMIndex;
import com.jivesoftware.os.lab.guts.LABIndexProvider;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.guts.ReadOnlyFile;
import com.jivesoftware.os.lab.guts.StripingBolBufferLocks;
import com.jivesoftware.os.lab.guts.allocators.LABAppendOnlyAllocator;
import com.jivesoftware.os.lab.guts.allocators.LABConcurrentSkipListMap;
import com.jivesoftware.os.lab.guts.allocators.LABConcurrentSkipListMemory;
import com.jivesoftware.os.lab.guts.allocators.LABIndexableMemory;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IPointerReadable;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;

/**
 * @author jonathan.colt
 */
public class LABEnvironment {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final File labRoot;
    private final LABStats stats;
    private final ExecutorService scheduler;
    private final ExecutorService compact;
    private final ExecutorService destroy;
    private final LabHeapPressure labHeapPressure;
    private final int minMergeDebt;
    private final int maxMergeDebt;
    private final LRUConcurrentBAHLinkedHash<Leaps> leapsCache;
    private final boolean useIndexableMemory;
    private final boolean fsyncFileRenames;

    private final String walName;
    private final LabWAL wal;
    private final Map<String, FormatTransformerProvider> formatTransformerProviderRegistry = Maps.newConcurrentMap();
    private final Map<String, Rawhide> rawhideRegistry = Maps.newConcurrentMap();
    private final Map<String, RawEntryFormat> rawEntryFormatRegistry = Maps.newConcurrentMap();

    private final StripingBolBufferLocks stripingBolBufferLocks;

    public static ExecutorService buildLABHeapSchedulerThreadPool(int count) {
        return Executors.newFixedThreadPool(count,
            new ThreadFactoryBuilder().setNameFormat("lab-heap-%d").build());
    }

    public static ExecutorService buildLABSchedulerThreadPool(int count) {
        return Executors.newFixedThreadPool(count,
            new ThreadFactoryBuilder().setNameFormat("lab-scheduler-%d").build());
    }

    public static ExecutorService buildLABCompactorThreadPool(int count) {
        return Executors.newFixedThreadPool(count,
            new ThreadFactoryBuilder().setNameFormat("lab-compact-%d").build());
    }

    public static ExecutorService buildLABDestroyThreadPool(int count) {
        return Executors.newFixedThreadPool(count,
            new ThreadFactoryBuilder().setNameFormat("lab-destroy-%d").build());
    }

    public static LRUConcurrentBAHLinkedHash<Leaps> buildLeapsCache(int maxCapacity, int concurrency) {
        return new LRUConcurrentBAHLinkedHash<>(10, maxCapacity, 0.5f, true, concurrency);
    }

    public LABEnvironment(LABStats stats,
        ExecutorService scheduler,
        ExecutorService compact,
        final ExecutorService destroy,
        String walName,
        long maxWALSizeInBytes,
        long maxEntriesPerWAL,
        long maxEntrySizeInBytes,
        long maxValueIndexHeapPressureOverride,
        File labRoot,
        LabHeapPressure labHeapPressure,
        int minMergeDebt,
        int maxMergeDebt,
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache,
        StripingBolBufferLocks bolBufferLocks,
        boolean useIndexableMemory,
        boolean fsyncFileRenames) throws IOException {

        register(NoOpFormatTransformerProvider.NAME, NoOpFormatTransformerProvider.NO_OP);
        register(KeyValueRawhide.NAME, KeyValueRawhide.SINGLETON);
        register(LABRawhide.NAME, LABRawhide.SINGLETON);
        register(MemoryRawEntryFormat.NAME, MemoryRawEntryFormat.SINGLETON);

        this.stats = stats;
        this.scheduler = scheduler;
        this.compact = compact;
        this.destroy = destroy;
        this.labRoot = labRoot;
        this.labHeapPressure = labHeapPressure;
        this.minMergeDebt = minMergeDebt;
        this.maxMergeDebt = maxMergeDebt;
        this.leapsCache = leapsCache;
        this.walName = walName;
        this.wal = new LabWAL(stats, new File(labRoot, walName), maxWALSizeInBytes, maxEntriesPerWAL, maxEntrySizeInBytes, maxValueIndexHeapPressureOverride);
        this.useIndexableMemory = useIndexableMemory;
        this.fsyncFileRenames = fsyncFileRenames;
        this.stripingBolBufferLocks = bolBufferLocks;
    }

    LabWAL getLabWAL() {
        return wal;
    }

    public void register(String name, FormatTransformerProvider formatTransformerProvider) {
        FormatTransformerProvider had = formatTransformerProviderRegistry.putIfAbsent(name, formatTransformerProvider);
        if (had != null) {
            throw new IllegalArgumentException("FormatTransformerProvider:" + had + " is already register under the name:" + name);
        }
    }

    FormatTransformerProvider formatTransformerProvider(String name) {
        return formatTransformerProviderRegistry.get(name);
    }

    public void register(String name, Rawhide rawhide) {
        Rawhide had = rawhideRegistry.putIfAbsent(name, rawhide);
        if (had != null) {
            throw new IllegalArgumentException("Rawhide:" + had + " is already register under the name:" + name);
        }
    }

    Rawhide rawhide(String name) {
        return rawhideRegistry.get(name);
    }

    public void register(String name, RawEntryFormat rawEntryFormat) {
        RawEntryFormat had = rawEntryFormatRegistry.putIfAbsent(name, rawEntryFormat);
        if (had != null) {
            throw new IllegalArgumentException("RawEntryFormat:" + had + " is already register under the name:" + name);
        }
    }

    RawEntryFormat rawEntryFormat(String name) {
        return rawEntryFormatRegistry.get(name);
    }

    public void open() throws Exception {
        this.wal.open(this);
    }

    public void close() throws Exception {
        this.wal.close(this);
    }

    public List<String> list() {
        List<String> indexes = Lists.newArrayList();
        File[] files = labRoot.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory() && !file.getName().equals(walName)) {
                    indexes.add(file.getName());
                }
            }
        }
        return indexes;
    }

    ValueIndexConfig valueIndexConfig(byte[] valueIndexId) throws Exception {
        String primaryName = new String(valueIndexId, StandardCharsets.UTF_8);
        File configFile = new File(labRoot, primaryName + ".json");
        if (configFile.exists()) {
            return MAPPER.readValue(configFile, ValueIndexConfig.class);
        } else {
            throw new IllegalStateException("There is no config for lab value index:" + primaryName);
        }
    }

    public ValueIndex open(ValueIndexConfig config) throws Exception {

        if (config.primaryName.equals(walName)) {
            throw new IllegalStateException("primaryName:" + config.primaryName + " cannot collide with walName");
        }

        FormatTransformerProvider formatTransformerProvider = formatTransformerProviderRegistry.get(config.formatTransformerProviderName);
        Preconditions.checkNotNull(formatTransformerProvider, "No FormatTransformerProvider registered for " + config.formatTransformerProviderName);
        Rawhide rawhide = rawhideRegistry.get(config.rawhideName);
        Preconditions.checkNotNull(formatTransformerProvider, "No Rawhide registered for " + config.rawhideName);
        RawEntryFormat rawEntryFormat = rawEntryFormatRegistry.get(config.rawEntryFormatName);
        Preconditions.checkNotNull(formatTransformerProvider, "No RawEntryFormat registered for " + config.rawEntryFormatName);

        File configFile = new File(labRoot, config.primaryName + ".json");
        if (configFile.exists()) {
            byte[] configAsBytes = MAPPER.writeValueAsBytes(config);
            ReadOnlyFile readOnlyFile = new ReadOnlyFile(configFile);
            boolean equal = false;
            IPointerReadable pointerReadable = readOnlyFile.pointerReadable(-1);
            try {
                long l = pointerReadable.length();
                equal = configAsBytes.length == l;
                if (equal) {
                    for (int i = 0; i < l; i++) {
                        if (configAsBytes[i] != pointerReadable.read(i)) {
                            equal = false;
                            break;
                        }
                    }
                }

            } finally {
                pointerReadable.close();
            }

            if (!equal) {
                LOG.info("Updating config for {}", config.primaryName);
                FileUtils.deleteQuietly(configFile);
                AppendOnlyFile appendOnlyFile = new AppendOnlyFile(configFile);
                IAppendOnly appender = null;
                try {
                    appender = appendOnlyFile.appender();
                    appender.append(configAsBytes, 0, configAsBytes.length);
                } finally {
                    if (appender != null) {
                        appender.flush(true);
                        appender.close();
                    }
                }
            }

        } else {
            configFile.getParentFile().mkdirs();
            MAPPER.writeValue(configFile, config);
        }

        LABIndexProvider indexProvider = (rawhide1, poweredUpToHint) -> {
            if (useIndexableMemory && config.entryLengthPower > 0) {
                LABAppendOnlyAllocator allocator = new LABAppendOnlyAllocator(config.primaryName,
                    Math.max(config.entryLengthPower, (poweredUpToHint - config.entryLengthPower) / 2)
                );
                LABIndexableMemory memory = new LABIndexableMemory(config.rawhideName, allocator);
                LABConcurrentSkipListMemory skipListMemory = new LABConcurrentSkipListMemory(rawhide1, memory);
                return new LABConcurrentSkipListMap(stats, skipListMemory, stripingBolBufferLocks);
            } else {
                return new LABCSLMIndex(rawhide1, stripingBolBufferLocks);
            }
        };

        return new LAB(stats,
            formatTransformerProvider,
            config.rawhideName,
            rawhide,
            rawEntryFormat,
            scheduler,
            compact,
            destroy,
            labRoot,
            wal,
            config.primaryName.getBytes(StandardCharsets.UTF_8),
            config.primaryName,
            config.entriesBetweenLeaps,
            labHeapPressure,
            config.maxHeapPressureInBytes,
            minMergeDebt,
            maxMergeDebt,
            config.splitWhenKeysTotalExceedsNBytes,
            config.splitWhenValuesTotalExceedsNBytes,
            config.splitWhenValuesAndKeysTotalExceedsNBytes,
            leapsCache,
            indexProvider,
            fsyncFileRenames);

    }

    public boolean rename(String oldName, String newName) throws IOException {
        File oldFileName = new File(labRoot, oldName);
        File newFileName = new File(labRoot, newName);
        if (oldFileName.exists()) {
            Files.move(oldFileName.toPath(), newFileName.toPath(), StandardCopyOption.ATOMIC_MOVE);
            FileUtils.deleteDirectory(oldFileName);
            return true;
        } else {
            return false;
        }
    }

    public void remove(String primaryName) throws IOException {
        File fileName = new File(labRoot, primaryName);
        FileUtils.deleteDirectory(fileName);
    }

    public void delete() throws IOException {
        FileUtils.deleteDirectory(labRoot);
    }

    public void shutdown() throws InterruptedException {
        compact.shutdown();
        destroy.shutdown();
        compact.awaitTermination(30, TimeUnit.SECONDS);
        destroy.awaitTermination(30, TimeUnit.SECONDS);
    }
}
