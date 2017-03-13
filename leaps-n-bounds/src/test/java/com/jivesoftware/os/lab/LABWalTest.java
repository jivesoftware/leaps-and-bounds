package com.jivesoftware.os.lab;

import com.google.common.io.Files;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.LABEnvironmentNGTest.IdProvider;
import com.jivesoftware.os.lab.api.MemoryRawEntryFormat;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.api.rawhide.LABRawhide;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.guts.StripingBolBufferLocks;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by jonathan.colt on 3/3/17.
 */
public class LABWalTest {

    @Test
    public void testEnvWithWALAndMemMap() throws Exception {

        File root = new File(Files.createTempDir().getParentFile(), "labWal");
        root.mkdirs();
        System.out.println("Created root " + root);
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        LabHeapPressure labHeapPressure1 = new LabHeapPressure(new LABStats(),
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            1024,
            1024 * 1024 * 10,
            new AtomicLong(),
            LabHeapPressure.FreeHeapStrategy.mostBytesFirst);

        LABEnvironment env = new LABEnvironment(new LABStats(),
            LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4),
            LABEnvironment.buildLABDestroyThreadPool(1),
            new LabWALConfig("labWal",
                "labMeta",
                1024 * 1024 * 10,
                10000,
                1024 * 1024 * 10,
                1024 * 1024 * 10),
            root,
            labHeapPressure1, 4, 8, leapsCache,
            new StripingBolBufferLocks(1024),
            true,
            true);

        ValueIndexConfig valueIndexConfig = new ValueIndexConfig("foo", 4096, 1024 * 1024 * 10, -1, -1, -1,
            NoOpFormatTransformerProvider.NAME, LABRawhide.NAME, MemoryRawEntryFormat.NAME, 19, TestUtils.indexType, 2d, true);

        System.out.println("Created env");

        env.open();

        ValueIndex index = env.open(valueIndexConfig);

        AtomicInteger monotonic = new AtomicInteger(-1);
        index.rowScan((index1, key, timestamp, tombstoned, version, payload) -> {
            System.out.println("opening:" + (monotonic.get() + 1) + " vs " + payload.getLong(0));
            Assert.assertEquals(monotonic.get() + 1, payload.getLong(0));
            monotonic.set((int) payload.getLong(0));
            return true;
        }, true);

        if (monotonic.get() == -1) {
            monotonic.set(0);
        }

        System.out.println("Opened at monotonic:" + monotonic.get());

        IdProvider idProvider = new IdProvider() {
            @Override
            public int nextId() {
                return monotonic.getAndIncrement();
            }

            @Override
            public void reset() {
                monotonic.set(0);
            }
        };

        int batchCount = 100;
        int batchSize = 1_000;
        if (monotonic.get() != 0) {
            batchCount = 1;
        }

        System.out.println("Open env");
        index(index, "foo", idProvider, batchCount, batchSize, false);
        System.out.println("Indexed");

        AtomicInteger all = new AtomicInteger();
        index.rowScan((index1, key, timestamp, tombstoned, version, payload) -> {
            all.set((int) payload.getLong(0));
            return true;
        }, true);

        System.out.println();
        System.out.println("/----------------------------");
        System.out.println("| Finally:" + all.get() + " " + root);
        System.out.println("\\----------------------------");
        System.out.println();


    }

    private void index(ValueIndex index,
        String name,
        IdProvider idProvider,
        int commitCount,
        int batchCount,
        boolean commit) throws Exception {


        AtomicLong count = new AtomicLong();
        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        for (int c = 0; c < commitCount; c++) {
            long start = System.currentTimeMillis();
            int[] wroteV = new int[1];
            index.append((stream) -> {
                for (int i = 0; i < batchCount; i++) {
                    count.incrementAndGet();
                    int nextI = idProvider.nextId();
                    int nextV = nextI;
                    wroteV[0] = nextV;
                    stream.stream(-1,
                        UIO.longBytes(nextI, new byte[8], 0),
                        System.currentTimeMillis(),
                        false,
                        0,
                        UIO.longBytes(nextV, new byte[8], 0));
                }
                return true;
            }, true, rawEntryBuffer, keyBuffer);

            System.out.println("---->   Append Elapse:" + (System.currentTimeMillis() - start) + " " + wroteV[0]);
            if (commit) {
                start = System.currentTimeMillis();
                index.commit(true, true);
                System.out.println("Commit Elapse:" + (System.currentTimeMillis() - start));
            }
        }
    }

}
