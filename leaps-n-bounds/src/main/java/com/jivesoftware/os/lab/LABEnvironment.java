package com.jivesoftware.os.lab;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.guts.Leaps;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;

/**
 * @author jonathan.colt
 */
public class LABEnvironment {

    private final File rootFile;
    private final ExecutorService compact;
    private final ExecutorService destroy;
    private final boolean useMemMap;
    private final LabHeapPressure labHeapPressure;
    private final int minMergeDebt;
    private final int maxMergeDebt;
    private final LRUConcurrentBAHLinkedHash<Leaps> leapsCache;

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

    public LABEnvironment(ExecutorService compact,
        final ExecutorService destroy,
        File rootFile,
        boolean useMemMap,
        LabHeapPressure labHeapPressure,
        int minMergeDebt,
        int maxMergeDebt,
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache) {
        this.compact = compact;
        this.destroy = destroy;
        this.rootFile = rootFile;
        this.useMemMap = useMemMap;
        this.labHeapPressure = labHeapPressure;
        this.minMergeDebt = minMergeDebt;
        this.maxMergeDebt = maxMergeDebt;
        this.leapsCache = leapsCache;
    }

    public ValueIndex open(String primaryName,
        int entriesBetweenLeaps,
        long maxHeapPressureInBytes,
        long splitWhenKeysTotalExceedsNBytes,
        long splitWhenValuesTotalExceedsNBytes,
        long splitWhenValuesAndKeysTotalExceedsNBytes,
        Rawhide rawhide,
        RawEntryFormat rawhideFormat) throws Exception {
        return new LAB(rawhide,
            rawhideFormat,
            compact,
            destroy,
            rootFile,
            primaryName,
            useMemMap,
            entriesBetweenLeaps,
            labHeapPressure,
            maxHeapPressureInBytes,
            minMergeDebt,
            maxMergeDebt,
            splitWhenKeysTotalExceedsNBytes,
            splitWhenValuesTotalExceedsNBytes,
            splitWhenValuesAndKeysTotalExceedsNBytes,
            leapsCache);
    }

    public boolean rename(String oldName, String newName) throws IOException {
        File oldFileName = new File(rootFile, oldName);
        File newFileName = new File(rootFile, newName);
        if (oldFileName.exists()) {
            Files.move(oldFileName.toPath(), newFileName.toPath(), StandardCopyOption.ATOMIC_MOVE);
            FileUtils.deleteDirectory(oldFileName);
            return true;
        } else {
            return false;
        }
    }

    public void remove(String primaryName) throws IOException {
        File fileName = new File(rootFile, primaryName);
        FileUtils.deleteDirectory(fileName);
    }

    public void delete() throws IOException {
        FileUtils.deleteDirectory(rootFile);
    }

    public void shutdown() throws InterruptedException {
        compact.shutdown();
        destroy.shutdown();
        compact.awaitTermination(30, TimeUnit.SECONDS);
        destroy.awaitTermination(30, TimeUnit.SECONDS);
    }
}
