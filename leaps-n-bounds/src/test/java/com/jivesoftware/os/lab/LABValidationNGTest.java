package com.jivesoftware.os.lab;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.exceptions.LABClosedException;
import com.jivesoftware.os.lab.api.rawhide.LABRawhide;
import com.jivesoftware.os.lab.guts.LABCSLMIndex;
import com.jivesoftware.os.lab.guts.LABIndexProvider;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.guts.StripingBolBufferLocks;
import com.jivesoftware.os.lab.guts.allocators.LABAppendOnlyAllocator;
import com.jivesoftware.os.lab.guts.allocators.LABConcurrentSkipListMap;
import com.jivesoftware.os.lab.guts.allocators.LABConcurrentSkipListMemory;
import com.jivesoftware.os.lab.guts.allocators.LABIndexableMemory;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class LABValidationNGTest {

    @Test(enabled = true, invocationCount = 1, singleThreaded = true)
    public void testClose() throws Exception {

        ExecutorService compact = Executors.newFixedThreadPool(2,
            new ThreadFactoryBuilder().setNameFormat("lab-compact-%d").build());

        ExecutorService destroy = Executors.newFixedThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("lab-destroy-%d").build());

        ExecutorService scheduler = LABEnvironment.buildLABSchedulerThreadPool(1);

        File walRoot = com.google.common.io.Files.createTempDir();
        File root = com.google.common.io.Files.createTempDir();
        File finalRoot = com.google.common.io.Files.createTempDir();
        int entriesBetweenLeaps = 2;
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        LABStats labStats = new LABStats();
        LabHeapPressure labHeapPressure = new LabHeapPressure(labStats,
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            1024 * 1024 * 10,
            1024 * 1024 * 10,
            new AtomicLong(),
            LabHeapPressure.FreeHeapStrategy.mostBytesFirst);

        StripingBolBufferLocks stripingBolBufferLocks = new StripingBolBufferLocks(1024);

        LabWAL wal = new LabWAL(labStats, walRoot, 1024 * 1024 * 10, 1000, 1024 * 1024 * 10, 1024 * 1024 * 10);

        LABIndexProvider indexProvider = (rawhide1, poweredUpTo) -> {
            if (true) {
                LABAppendOnlyAllocator allocator = new LABAppendOnlyAllocator("test", Math.max(3, poweredUpTo));
                LABIndexableMemory memory = new LABIndexableMemory( allocator);
                LABConcurrentSkipListMemory skipList = new LABConcurrentSkipListMemory(rawhide1, memory);
                return new LABConcurrentSkipListMap(labStats, skipList, stripingBolBufferLocks);
            } else {
                return new LABCSLMIndex(rawhide1, stripingBolBufferLocks);
            }
        };

        LAB lab = new LAB(labStats,
            NoOpFormatTransformerProvider.NO_OP,
            LABRawhide.NAME,
            LABRawhide.SINGLETON,
            new RawEntryFormat(0, 0),
            scheduler,
            compact,
            destroy,
            root,
            wal,
            "lab".getBytes(),
            "lab",
            entriesBetweenLeaps,
            labHeapPressure,
            1024 * 1024 * 10,
            4,
            8,
            128,
            0,
            0,
            leapsCache,
            indexProvider,
            false,
            TestUtils.indexType,
            0.75d,
            true);

        int writerCount = 12;
        ExecutorService writers = Executors.newFixedThreadPool(writerCount, new ThreadFactoryBuilder().setNameFormat("writers-%d").build());
        int commitCount = 100;
        int batchSize = 1;
        AtomicLong value = new AtomicLong();
        AtomicLong version = new AtomicLong();
        AtomicLong count = new AtomicLong();
        boolean fsync = true;
        AtomicBoolean close = new AtomicBoolean(false);

        AtomicLong nextId = new AtomicLong();
        AtomicLong running = new AtomicLong();
        List<Future> writerFutures = new ArrayList<>();
        for (int i = 0; i < writerCount; i++) {
            int wi = i;
            running.incrementAndGet();
            writerFutures.add(writers.submit(() -> {
                try {
                    BolBuffer rawEntryBuffer = new BolBuffer();
                    BolBuffer keyBuffer = new BolBuffer();
                    for (int c = 0; c < commitCount; c++) {

                        if (version.get() > (writerCount * commitCount * batchSize) / 2) {
                            if (close.compareAndSet(false, true)) {
                                System.out.println("****** Closing lab during writes... ****** ");
                                lab.close(true, fsync);
                                System.out.println("****** Lab closed... ****** ");

                                try {
                                    FileUtils.forceMkdir(new File(finalRoot, "foobar"));
                                    Files.move(root.toPath(), new File(finalRoot, "foobar").toPath(), StandardCopyOption.ATOMIC_MOVE);
                                } catch (Exception x) {
                                    Assert.fail();
                                }
                                System.out.println("****** Lab moved... ****** ");
                            }
                        }
                        lab.append((stream) -> {
                            for (int b = 0; b < batchSize; b++) {
                                count.incrementAndGet();
                                stream.stream(-1,
                                    UIO.longBytes(nextId.incrementAndGet(), new byte[8], 0),
                                    System.currentTimeMillis(),
                                    false,
                                    version.incrementAndGet(),
                                    UIO.longBytes(value.incrementAndGet(), new byte[8], 0));
                            }
                            return true;
                        }, fsync, rawEntryBuffer, keyBuffer);

                        lab.commit(fsync, true);
                    }
                    System.out.println("Writer " + wi + " done...");
                    return null;
                } catch (Exception x) {
                    if (close.get() && (x instanceof LABClosedException)) {
                        System.out.println("Writer " + wi + " exiting because: " + x);
                        return null;
                    } else {
                        x.printStackTrace();
                        throw x;
                    }
                } finally {
                    running.decrementAndGet();
                }
            }));
        }

        for (Future f : writerFutures) {
            f.get();
        }
        writers.shutdownNow();
        scheduler.shutdownNow();
        compact.shutdownNow();
        destroy.shutdownNow();

    }

    @Test(enabled = true, invocationCount = 1, singleThreaded = true)
    public void testConcurrencyMethod() throws Exception {

        ExecutorService compact = Executors.newFixedThreadPool(2,
            new ThreadFactoryBuilder().setNameFormat("lab-compact-%d").build());

        ExecutorService destroy = Executors.newFixedThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("lab-destroy-%d").build());

        ExecutorService scheduler = LABEnvironment.buildLABSchedulerThreadPool(1);

        File walRoot = com.google.common.io.Files.createTempDir();
        File root = com.google.common.io.Files.createTempDir();
        int entriesBetweenLeaps = 2;
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        LABStats labStats = new LABStats();
        LabHeapPressure labHeapPressure = new LabHeapPressure(labStats,
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            1024 * 1024 * 10,
            1024 * 1024 * 10,
            new AtomicLong(),
            LabHeapPressure.FreeHeapStrategy.mostBytesFirst);

        LabWAL wal = new LabWAL(labStats, walRoot, 1024 * 1024 * 10, 1000, 1024 * 1024 * 10, 1024 * 1024 * 10);

        StripingBolBufferLocks stripingBolBufferLocks = new StripingBolBufferLocks(1024);

        LABRawhide rawhide = LABRawhide.SINGLETON;

        LABIndexProvider indexProvider = (rawhide1, poweredUpTo) -> {
            if (true) {
                LABAppendOnlyAllocator allocator = new LABAppendOnlyAllocator("test", Math.max(3, poweredUpTo));
                LABIndexableMemory memory = new LABIndexableMemory(allocator);
                LABConcurrentSkipListMemory skipList = new LABConcurrentSkipListMemory(rawhide1, memory);
                return new LABConcurrentSkipListMap(labStats, skipList, stripingBolBufferLocks);
            } else {
                return new LABCSLMIndex(rawhide1, stripingBolBufferLocks);
            }
        };

        LAB lab = new LAB(
            labStats,
            NoOpFormatTransformerProvider.NO_OP,
            LABRawhide.NAME, rawhide,
            new RawEntryFormat(0, 0),
            scheduler,
            compact,
            destroy,
            root,
            wal,
            "lab".getBytes(),
            "lab",
            entriesBetweenLeaps,
            labHeapPressure,
            1024 * 1024 * 10,
            4, 8, 128, 0, 0,
            leapsCache,
            indexProvider,
            false,
            TestUtils.indexType,
            0.75d,
            true);

        validationTest(lab);

        lab.close(true, true);
        scheduler.shutdownNow();
        compact.shutdownNow();
        destroy.shutdownNow();

    }

    private void validationTest(LAB lab) throws InterruptedException, Exception, ExecutionException {
        int writerCount = 30;
        int readerCount = 2;

        AtomicLong hits = new AtomicLong();
        AtomicLong version = new AtomicLong();
        AtomicLong value = new AtomicLong();
        AtomicLong count = new AtomicLong();

        int commitCount = 30;
        int batchSize = 1;
        boolean fsync = true;

        ExecutorService writers = Executors.newFixedThreadPool(writerCount, new ThreadFactoryBuilder().setNameFormat("writers-%d").build());
        ExecutorService readers = Executors.newFixedThreadPool(readerCount, new ThreadFactoryBuilder().setNameFormat("readers-%d").build());

        AtomicLong nextId = new AtomicLong();
        AtomicLong running = new AtomicLong();
        List<Future> writerFutures = new ArrayList<>();
        for (int i = 0; i < writerCount; i++) {
            running.incrementAndGet();
            writerFutures.add(writers.submit(() -> {
                try {
                    BolBuffer rawEntryBuffer = new BolBuffer();
                    BolBuffer keyBuffer = new BolBuffer();
                    for (int c = 0; c < commitCount; c++) {
                        lab.append((stream) -> {
                            for (int b = 0; b < batchSize; b++) {
                                count.incrementAndGet();
                                stream.stream(-1,
                                    UIO.longBytes(nextId.incrementAndGet(), new byte[8], 0),
                                    System.currentTimeMillis(),
                                    false,
                                    version.incrementAndGet(),
                                    UIO.longBytes(value.incrementAndGet(), new byte[8], 0));
                            }
                            return true;
                        }, fsync, rawEntryBuffer, keyBuffer);
                        lab.commit(fsync, true);
                        System.out.println((c + 1) + " out of " + commitCount + " gets:" + hits.get() + " debt:" + lab.debt());
                    }
                    return null;
                } catch (Exception x) {
                    x.printStackTrace();
                    throw x;
                } finally {
                    running.decrementAndGet();
                }
            }));
        }

        AtomicLong passed = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        List<String> log = new ArrayList<>();

        List<Future> readerFutures = new ArrayList<>();
        for (int r = 0; r < readerCount; r++) {
            int readerId = r;
            readerFutures.add(readers.submit(() -> {
                try {
                    int overRun = 25;
                    while (running.get() > 0 || overRun > 0) {
                        long maxId = nextId.get();
                        if (maxId == 0) {
                            Thread.yield();
                            continue;
                        }
                        Set<Long> found = new HashSet<>();
                        for (int i = 1; i <= maxId; i++) {
                            int ii = i;
                            lab.get(
                                (keyStream) -> {
                                    byte[] key = UIO.longBytes(ii, new byte[8], 0);
                                    keyStream.key(0, key, 0, key.length);
                                    return true;
                                },
                                (index, key, timestamp, tombstoned, version1, value1) -> {
                                    hits.incrementAndGet();
                                    found.add(key == null ? 0 : key.getLong(0));
                                    return true;
                                }, true);
                        }

                        if (maxId == found.size()) {
                            passed.incrementAndGet();
                            System.out.println("PASSED: " + found.size() + "  vs " + maxId);
                            log.add("PASSED: " + found.size() + "  vs " + maxId);
                            failed.set(0);
                        } else {
                            failed.incrementAndGet();
                            List<Long> missing = new ArrayList<>();
                            for (long i = 1; i <= maxId; i++) {
                                if (!found.contains(i)) {
                                    missing.add(i);
                                }
                            }
                            System.out.println("FAILED: " + found.size() + "  vs " + maxId + " missing=" + missing + " " + missing.size());
                            log.add("FAILED: " + found.size() + "  vs " + maxId + " missing=" + missing + " " + missing.size());
                        }
                        if (running.get() <= 0) {
                            overRun--;
                            Thread.sleep(10);
                        }

                    }
                    System.out.println("Reader (" + readerId + ") finished.");
                    return null;
                } catch (Exception x) {
                    log.add(x.getMessage());
                    x.printStackTrace();
                    throw x;
                }
            }));
        }

        for (Future f : writerFutures) {
            f.get();
        }

        for (Future f : readerFutures) {
            f.get();
        }
        System.out.println("------------------------- READERS ARE DONE -----------------------");

        System.out.println("------------------------------------------------------------------");
        System.out.println("------------------------- ALL DONE -------------------------------");
        System.out.println("------------------------------------------------------------------");

        writers.shutdownNow();
        readers.shutdownNow();

        if (failed.get() > 0) {
            for (String failure : log) {
                System.out.println(":( " + failure);
            }
            Assert.fail();
        }
    }

}
