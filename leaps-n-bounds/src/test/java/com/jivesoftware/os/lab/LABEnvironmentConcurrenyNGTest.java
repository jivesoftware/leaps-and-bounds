package com.jivesoftware.os.lab;

import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.MemoryRawEntryFormat;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABEnvironmentConcurrenyNGTest {

    @Test(enabled = true)
    public void testConcurrencyMethod() throws Exception {

        File root = Files.createTempDir();
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        LabHeapPressure labHeapPressure = new LabHeapPressure(1024 * 1024 * 10, new AtomicLong());
        LABEnvironment env = new LABEnvironment(LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4),
            LABEnvironment.buildLABDestroyThreadPool(1),
            "wal",
            1024 * 1024 * 10,
            1000,
            1024 * 1024 * 10,
            root,
            labHeapPressure,
            4,
            10,
            leapsCache);

        concurentTest(env);
    }

    @Test(enabled = true)
    public void testConcurrencyWithMemMapMethod() throws Exception {

        File root = Files.createTempDir();
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        LabHeapPressure labHeapPressure = new LabHeapPressure(1024 * 1024 * 10, new AtomicLong());
        LABEnvironment env = new LABEnvironment(LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4),
            LABEnvironment.buildLABDestroyThreadPool(1),
            "wal",
            1024 * 1024 * 10,
            1000,
            1024 * 1024 * 10,
            root,
            labHeapPressure,
            4,
            10,
            leapsCache);

        concurentTest(env);
    }

    private void concurentTest(LABEnvironment env) throws Exception {
        int writerCount = 16;
        int readerCount = 16;

        AtomicLong hits = new AtomicLong();
        AtomicLong version = new AtomicLong();
        AtomicLong value = new AtomicLong();
        AtomicLong count = new AtomicLong();

        int totalCardinality = 100_000_000;
        int commitCount = 10;
        int batchSize = 1000;
        boolean fsync = true;

        ExecutorService writers = Executors.newFixedThreadPool(writerCount, new ThreadFactoryBuilder().setNameFormat("writers-%d").build());
        ExecutorService readers = Executors.newFixedThreadPool(readerCount, new ThreadFactoryBuilder().setNameFormat("readers-%d").build());

        Random rand = new Random(12345);
        ValueIndexConfig valueIndexConfig = new ValueIndexConfig("foo", 4096, 1000, 10 * 1024 * 1024, 0, 0,
            NoOpFormatTransformerProvider.NAME, LABRawhide.NAME, MemoryRawEntryFormat.NAME);
        ValueIndex index = env.open(valueIndexConfig);

        AtomicLong running = new AtomicLong();
        List<Future> writerFutures = new ArrayList<>();
        for (int i = 0; i < writerCount; i++) {
            running.incrementAndGet();
            writerFutures.add(writers.submit(() -> {
                try {
                    for (int c = 0; c < commitCount; c++) {
                        index.append((ValueStream stream) -> {
                            for (int b = 0; b < batchSize; b++) {
                                count.incrementAndGet();
                                stream.stream(-1,
                                    UIO.longBytes(rand.nextInt(totalCardinality)),
                                    System.currentTimeMillis(),
                                    rand.nextBoolean(),
                                    version.incrementAndGet(),
                                    UIO.longBytes(value.incrementAndGet()));
                            }
                            return true;
                        }, fsync);
                        index.commit(fsync, true);
                        System.out.println((c + 1) + " out of " + commitCount + " gets:" + hits.get() + " debt:" + index.debt());
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

        List<Future> readerFutures = new ArrayList<>();
        for (int r = 0; r < readerCount; r++) {
            int readerId = r;
            readerFutures.add(readers.submit(() -> {
                try {

                    while (running.get() > 0) {
                        index.get(
                            (keyStream) -> {
                                byte[] key = UIO.longBytes(rand.nextInt(1_000_000));
                                keyStream.key(0, key, 0, key.length);
                                return true;
                            },
                            (index1, key, timestamp, tombstoned, version1, value1) -> {
                                hits.incrementAndGet();
                                return true;
                            }, true);
                    }
                    System.out.println("Reader (" + readerId + ") finished.");
                    return null;
                } catch (Exception x) {
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

        writers.shutdown();
        readers.shutdown();
        System.out.println("ALL DONE");
        env.shutdown();
    }

}
