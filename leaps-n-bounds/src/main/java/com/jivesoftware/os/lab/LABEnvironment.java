package com.jivesoftware.os.lab;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.FormatTransformerProvider;
import com.jivesoftware.os.lab.api.KeyValueRawhide;
import com.jivesoftware.os.lab.api.MemoryRawEntryFormat;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.wal.WAL;
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

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
    private final Cache<String, ValueIndex> openValueIndexsCache = CacheBuilder.<String, ValueIndex>newBuilder().weakValues().build();

    private final File labRoot;
    private final ExecutorService scheduler;
    private final ExecutorService compact;
    private final ExecutorService destroy;
    private final boolean useMemMap;
    private final LabHeapPressure labHeapPressure;
    private final int minMergeDebt;
    private final int maxMergeDebt;
    private final LRUConcurrentBAHLinkedHash<Leaps> leapsCache;

    private final WAL wal;
    private final Map<String, FormatTransformerProvider> formatTransformerProviderRegistry = Maps.newConcurrentMap();
    private final Map<String, Rawhide> rawhideRegistry = Maps.newConcurrentMap();
    private final Map<String, RawEntryFormat> rawEntryFormatRegistry = Maps.newConcurrentMap();

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

    public LABEnvironment(ExecutorService scheduler,
        ExecutorService compact,
        final ExecutorService destroy,
        File walRoot,
        long maxRowSizeInBytes,
        File labRoot,
        boolean useMemMap,
        LabHeapPressure labHeapPressure,
        int minMergeDebt,
        int maxMergeDebt,
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache) throws IOException {

        register(NoOpFormatTransformerProvider.NAME, NoOpFormatTransformerProvider.NO_OP);
        register(KeyValueRawhide.NAME, KeyValueRawhide.SINGLETON);
        register(LABRawhide.NAME, LABRawhide.SINGLETON);
        register(MemoryRawEntryFormat.NAME, MemoryRawEntryFormat.SINGLETON);

        this.scheduler = scheduler;
        this.compact = compact;
        this.destroy = destroy;
        this.labRoot = labRoot;
        this.useMemMap = useMemMap;
        this.labHeapPressure = labHeapPressure;
        this.minMergeDebt = minMergeDebt;
        this.maxMergeDebt = maxMergeDebt;
        this.leapsCache = leapsCache;
        this.wal = new WAL(walRoot, maxRowSizeInBytes);

    }

    public void register(String name, FormatTransformerProvider formatTransformerProvider) {
        FormatTransformerProvider had = formatTransformerProviderRegistry.putIfAbsent(name, formatTransformerProvider);
        if (had != null) {
            throw new IllegalArgumentException("FormatTransformerProvider:" + had + " is already register under the name:" + name);
        }
    }

    public void register(String name, Rawhide rawhide) {
        Rawhide had = rawhideRegistry.putIfAbsent(name, rawhide);
        if (had != null) {
            throw new IllegalArgumentException("Rawhide:" + had + " is already register under the name:" + name);
        }
    }

    public void register(String name, RawEntryFormat rawEntryFormat) {
        RawEntryFormat had = rawEntryFormatRegistry.putIfAbsent(name, rawEntryFormat);
        if (had != null) {
            throw new IllegalArgumentException("RawEntryFormat:" + had + " is already register under the name:" + name);
        }
    }

    public void open() throws IOException {
        this.wal.open(this);
    }

    public List<String> list() {
        List<String> indexes = Lists.newArrayList();
        File[] files = labRoot.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    indexes.add(file.getName());
                }
            }
        }
        return indexes;
    }

    ValueIndex open(byte[] valueIndexId) throws Exception {
        String primaryName = new String(valueIndexId, StandardCharsets.UTF_8);
        File configFile = new File(labRoot, primaryName + ".json");
        if (configFile.exists()) {
            return open(mapper.readValue(configFile, ValueIndexConfig.class));
        } else {
            throw new IllegalStateException("There is no config for lab value index:" + primaryName);
        }
    }

    public ValueIndex open(ValueIndexConfig config) throws Exception {

        return openValueIndexsCache.get(config.primaryName, () -> {

            FormatTransformerProvider formatTransformerProvider = formatTransformerProviderRegistry.get(config.formatTransformerProviderName);
            Preconditions.checkNotNull(formatTransformerProvider, "No FormatTransformerProvider registered for " + config.formatTransformerProviderName);
            Rawhide rawhide = rawhideRegistry.get(config.rawhideName);
            Preconditions.checkNotNull(formatTransformerProvider, "No Rawhide registered for " + config.rawhideName);
            RawEntryFormat rawEntryFormat = rawEntryFormatRegistry.get(config.rawEntryFormatName);
            Preconditions.checkNotNull(formatTransformerProvider, "No RawEntryFormat registered for " + config.rawEntryFormatName);

            File configFile = new File(labRoot, config.primaryName + ".json");
            if (configFile.exists()) {
                // TODO read compare and other cool stuff
                mapper.writeValue(configFile, config);
            } else {
                mapper.writeValue(configFile, config);
            }

            return new LAB(formatTransformerProvider,
                rawhide,
                rawEntryFormat,
                scheduler,
                compact,
                destroy,
                labRoot,
                wal,
                config.primaryName.getBytes(StandardCharsets.UTF_8),
                config.primaryName,
                useMemMap,
                config.entriesBetweenLeaps,
                labHeapPressure,
                config.maxHeapPressureInBytes,
                minMergeDebt,
                maxMergeDebt,
                config.splitWhenKeysTotalExceedsNBytes,
                config.splitWhenValuesTotalExceedsNBytes,
                config.splitWhenValuesAndKeysTotalExceedsNBytes,
                leapsCache);
        });

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
