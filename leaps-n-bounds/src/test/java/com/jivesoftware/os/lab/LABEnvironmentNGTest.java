package com.jivesoftware.os.lab;

import com.google.common.io.Files;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.MemoryRawEntryFormat;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.Assert.assertEquals;

/**
 * @author jonathan.colt
 */
public class LABEnvironmentNGTest {

    @Test
    public void testEnv() throws Exception {

        File root = null;
        try {
            IdProvider idProvider = new RandomIdProvider(12345, 100_000_000);
            //IdProvider idProvider = new MonotomicIdProvider(0);

            root = Files.createTempDir();
            System.out.println("root" + root.getAbsolutePath());
            LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
            LabHeapPressure labHeapPressure = new LabHeapPressure(LABEnvironment.buildLABHeapSchedulerThreadPool(1), "default", 1024 * 1024 * 10,
                1024 * 1024 * 10,
                new AtomicLong());
            LABEnvironment env = new LABEnvironment(LABEnvironment.buildLABSchedulerThreadPool(1),
                LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1),
                "wal", 1024 * 1024 * 10,
                1000, 1024 * 1024 * 10,
                1024 * 1024 * 10, root,
                labHeapPressure, 4, 8, leapsCache,false);
            assertEquals(env.list(), Collections.emptyList());

            ValueIndexConfig valueIndexConfig = new ValueIndexConfig("foo", 4096, 1024 * 1024 * 10, -1, -1, -1,
                NoOpFormatTransformerProvider.NAME, LABRawhide.NAME, MemoryRawEntryFormat.NAME);

            ValueIndex index = env.open(valueIndexConfig);
            System.out.println("Lets index so stuff....");
            index(index, "foo", idProvider, 34, 3_000, false, true);
            System.out.println("Lets test its all there....");

            test(index, "foo", idProvider, 34, 3_000);
            assertEquals(env.list(), Collections.singletonList("foo"));

            labHeapPressure = new LabHeapPressure(LABEnvironment.buildLABHeapSchedulerThreadPool(1), "default", 1024 * 1024 * 10, 1024 * 1024 * 10,
                new AtomicLong());
            env = new LABEnvironment(LABEnvironment.buildLABSchedulerThreadPool(1),
                LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1),
                "wal", 1024 * 1024 * 10,
                1000, 1024 * 1024 * 10,
                1024 * 1024 * 10, root,
                labHeapPressure, 4, 8, leapsCache,false);

            index = env.open(valueIndexConfig);
            assertEquals(env.list(), Collections.singletonList("foo"));
            System.out.println("\nLets re-test its all there....");
            test(index, "foo", idProvider, 34, 3_000);
            System.out.println("Lets re-index so stuff....");
            index(index, "foo", idProvider, 34, 3_000, false, true);
            System.out.println("Lets re-re-test its all there....");
            test(index, "foo", idProvider, 34, 3_000);

            env.shutdown();

            labHeapPressure = new LabHeapPressure(LABEnvironment.buildLABHeapSchedulerThreadPool(1), "default", 1024 * 1024 * 10, 1024 * 1024 * 10,
                new AtomicLong());
            env = new LABEnvironment(LABEnvironment.buildLABSchedulerThreadPool(1),
                LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1),
                "wal", 1024 * 1024 * 10,
                1000, 1024 * 1024 * 10,
                1024 * 1024 * 10, root,
                labHeapPressure, 4, 8, leapsCache,false);
            assertEquals(env.list(), Collections.singletonList("foo"));
            env.rename("foo", "bar");

            valueIndexConfig = new ValueIndexConfig("bar", 4096, 1024 * 1024 * 10, -1, -1, -1,
                NoOpFormatTransformerProvider.NAME, LABRawhide.NAME, MemoryRawEntryFormat.NAME);
            assertEquals(env.list(), Collections.singletonList("bar"));
            index = env.open(valueIndexConfig);

            System.out.println("Lets test its all there after move....");
            test(index, "bar", idProvider, 34, 3_000);
            System.out.println("Lets re-re-re-index so stuff after move....");
            index(index, "bar", idProvider, 34, 3_000, false, true);
            System.out.println("Lets re-test its all there after move....");
            test(index, "bar", idProvider, 34, 3_000);

            env.shutdown();
            labHeapPressure = new LabHeapPressure(LABEnvironment.buildLABHeapSchedulerThreadPool(1), "default", 1024 * 1024 * 10, 1024 * 1024 * 10,
                new AtomicLong());
            env = new LABEnvironment(LABEnvironment.buildLABSchedulerThreadPool(1),
                LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1),
                "wal", 1024 * 1024 * 10,
                1000, 1024 * 1024 * 10,
                1024 * 1024 * 10, root,
                labHeapPressure, 4, 8, leapsCache,false);
            assertEquals(env.list(), Collections.singletonList("bar"));
            env.remove("bar");
            assertEquals(env.list(), Collections.emptyList());
        } catch (Throwable x) {
            System.out.println("________________________________________________________");
            System.out.println(printDirectoryTree(root));
            throw x;
        }

    }

    public static String printDirectoryTree(File folder) {
        if (!folder.isDirectory()) {
            throw new IllegalArgumentException("folder is not a Directory");
        }
        int indent = 0;
        StringBuilder sb = new StringBuilder();
        printDirectoryTree(folder, indent, sb);
        return sb.toString();
    }

    private static void printDirectoryTree(File folder, int indent,
        StringBuilder sb) {
        if (!folder.isDirectory()) {
            throw new IllegalArgumentException("folder is not a Directory");
        }
        sb.append(getIndentString(indent));
        sb.append("+--");
        sb.append(folder.getName());
        sb.append("/");
        sb.append("\n");
        for (File file : folder.listFiles()) {
            if (file.isDirectory()) {
                printDirectoryTree(file, indent + 1, sb);
            } else {
                printFile(file, indent + 1, sb);
            }
        }

    }

    private static void printFile(File file, int indent, StringBuilder sb) {
        sb.append(getIndentString(indent));
        sb.append("+--");
        sb.append(file.getName());
        sb.append("\n");
    }

    private static String getIndentString(int indent) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < indent; i++) {
            sb.append("|  ");
        }
        return sb.toString();
    }

    @Test
    public void testEnvWithMemMap() throws Exception {
        IdProvider idProvider = new RandomIdProvider(12345, 100_000_000);
        //IdProvider idProvider = new MonotomicIdProvider(0);

        File root = Files.createTempDir();
        System.out.println("Created root");
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        LabHeapPressure labHeapPressure = new LabHeapPressure(LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default", 1024 * 1024 * 10, 1024 * 1024 * 10, new AtomicLong());
        LABEnvironment env = new LABEnvironment(LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1),
            "wal", 1024 * 1024 * 10,
            1000, 1024 * 1024 * 10,
            1024 * 1024 * 10, root,
            labHeapPressure, 4, 8, leapsCache,false);

        ValueIndexConfig valueIndexConfig = new ValueIndexConfig("foo", 4096, 1024 * 1024 * 10, -1, -1, -1,
            NoOpFormatTransformerProvider.NAME, LABRawhide.NAME, MemoryRawEntryFormat.NAME);

        System.out.println("Created env");

        ValueIndex index = env.open(valueIndexConfig);
        System.out.println("Open env");
        index(index, "foo", idProvider, 34, 3_000, false, true);
        test(index, "foo", idProvider, 34, 3_000);
        System.out.println("Indexed");

        env.shutdown();
        System.out.println("Shutdown");

        labHeapPressure = new LabHeapPressure(LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default", 1024 * 1024 * 10, 1024 * 1024 * 10, new AtomicLong());
        env = new LABEnvironment(LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1),
            "wal", 1024 * 1024 * 10,
            1000, 1024 * 1024 * 10,
            1024 * 1024 * 10, root,
            labHeapPressure, 4, 8, leapsCache,false);
        System.out.println("Recreate env");

        index = env.open(valueIndexConfig);
        System.out.println("Re-open env");
        test(index, "foo", idProvider, 34, 3_000);
        index(index, "foo", idProvider, 34, 3_000, true, false);
        test(index, "foo", idProvider, 34, 3_000);
        System.out.println("Re-indexed");
        env.shutdown();
        System.out.println("Re-shutdown");

    }

    @Test
    public void testEnvWithWALAndMemMap() throws Exception {
        IdProvider idProvider = new RandomIdProvider(12345, 100_000_000);
        //IdProvider idProvider = new MonotomicIdProvider(0);

        File root = Files.createTempDir();
        System.out.println("Created root");
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        LabHeapPressure labHeapPressure = new LabHeapPressure(LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default", 1024 * 1024 * 10, 1024 * 1024 * 10,
            new AtomicLong());
        LABEnvironment env = new LABEnvironment(LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1),
            "wal", 1024 * 1024 * 10,
            1000,
            1024 * 1024 * 10,
            1024 * 1024 * 10, root,
            labHeapPressure, 4, 8, leapsCache,false);

        ValueIndexConfig valueIndexConfig = new ValueIndexConfig("foo", 4096, 1024 * 1024 * 10, -1, -1, -1,
            NoOpFormatTransformerProvider.NAME, LABRawhide.NAME, MemoryRawEntryFormat.NAME);

        System.out.println("Created env");

        ValueIndex index = env.open(valueIndexConfig);
        System.out.println("Open env");
        index(index, "foo", idProvider, 34, 3_000, true, false);
        test(index, "foo", idProvider, 34, 3_000);
        System.out.println("Indexed");
        env.close();
        env.shutdown();
        System.out.println("Shutdown");

        labHeapPressure = new LabHeapPressure(LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default", 1024 * 1024 * 10, 1024 * 1024 * 10, new AtomicLong());
        env = new LABEnvironment(LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1),
            "wal", 1024 * 1024 * 10,
            1000,
            1024 * 1024 * 10,
            1024 * 1024 * 10, root,
            labHeapPressure, 4, 8, leapsCache,false);
        System.out.println("Recreate env");
        env.open();
        System.out.println("Re-open index");
        index = env.open(valueIndexConfig);
        test(index, "foo", idProvider, 34, 3_000);
        index(index, "foo", idProvider, 34, 3_000, true, true);
        test(index, "foo", idProvider, 34, 3_000);
        System.out.println("Re-indexed");
        env.shutdown();
        System.out.println("Re-shutdown");

        labHeapPressure = new LabHeapPressure(LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default", 1024 * 1024 * 10, 1024 * 1024 * 10, new AtomicLong());
        env = new LABEnvironment(LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1),
            "wal", 1024 * 1024 * 10,
            1000,
            1024 * 1024 * 10,
            1024 * 1024 * 10, root,
            labHeapPressure, 4, 8, leapsCache,false);
        System.out.println("Re-Recreate env");
        env.open();
        System.out.println("Re-Re-open index");
        index = env.open(valueIndexConfig);
        test(index, "foo", idProvider, 34, 3_000);
        env.shutdown();
        System.out.println("Re-Re-shutdown");

    }

    private void index(ValueIndex index,
        String name,
        IdProvider idProvider,
        int commitCount,
        int batchCount,
        boolean useWAL,
        boolean commit) throws Exception {

        idProvider.reset();
        assertEquals(index.name(), name);

        AtomicLong version = new AtomicLong();
        AtomicLong value = new AtomicLong();
        AtomicLong count = new AtomicLong();

        for (int c = 0; c < commitCount; c++) {
            long start = System.currentTimeMillis();
            if (useWAL) {
                index.journaledAppend((stream) -> {
                    for (int i = 0; i < batchCount; i++) {
                        count.incrementAndGet();
                        int nextI = idProvider.nextId();
                        //System.out.print(nextI + ", ");
                        stream.stream(-1,
                            UIO.longBytes(nextI),
                            System.currentTimeMillis(),
                            false,
                            version.incrementAndGet(),
                            UIO.longBytes(value.incrementAndGet()));
                    }
                    return true;
                }, true);
            } else {
                index.append((stream) -> {
                    for (int i = 0; i < batchCount; i++) {
                        count.incrementAndGet();
                        int nextI = idProvider.nextId();
                        //System.out.print(nextI + ", ");
                        stream.stream(-1,
                            UIO.longBytes(nextI),
                            System.currentTimeMillis(),
                            false,
                            version.incrementAndGet(),
                            UIO.longBytes(value.incrementAndGet()));
                    }
                    return true;
                }, true);
            }

            System.out.println("Append Elapse:" + (System.currentTimeMillis() - start));
            if (commit) {
                start = System.currentTimeMillis();
                index.commit(true, true);
                System.out.println("Commit Elapse:" + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            }

        }

    }

    private void test(ValueIndex index, String name, IdProvider idProvider, int commitCount, int batchCount) throws Exception {
        if (index instanceof LAB) {
            System.out.println("\n\n");
            ((LAB) index).auditRanges((key) -> "" + UIO.bytesLong(key));
            System.out.println("\n\n");
        }

        idProvider.reset();
        assertEquals(index.name(), name);

        System.out.println("Testing Hits... " + name);
        List<String> failures = Lists.newArrayList();
        for (int c = 0; c < commitCount; c++) {
            for (int i = 0; i < batchCount; i++) {
                long askedFor = idProvider.nextId();

                index.get(
                    (keyStream) -> {
                        byte[] key = UIO.longBytes(askedFor);
                        keyStream.key(0, key, 0, key.length);
                        return true;
                    },
                    (index1, key, timestamp, tombstoned, version1, value1) -> {
                        if (value1 == null) {
                            failures.add(
                                " FAILED to find " + askedFor + " " + index1 + " "
                                + IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version1 + " " + IndexUtil.toString(value1)
                            );
                        }
                        return true;
                    }, true);
            }
            System.out.print(".");
        }

        System.out.println("\nTesting Misses... " + name);
        for (int c = 0; c < commitCount; c++) {
            for (int i = 0; i < batchCount; i++) {
                long askedFor = -idProvider.nextId();
                index.get(
                    (keyStream) -> {
                        byte[] key = UIO.longBytes(askedFor);
                        keyStream.key(0, key, 0, key.length);
                        return true;
                    },
                    (index1, key, timestamp, tombstoned, version1, value1) -> {
                        if (value1 != null) {
                            failures.add(
                                " FAILED to miss " + askedFor + " " + index1 + " "
                                + IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version1 + " " + IndexUtil.toString(value1)
                            );
                        }
                        return true;
                    }, true);
            }
            System.out.print(".");
        }

        if (!failures.isEmpty()) {

            Assert.fail(failures.toString());
        }
    }

    static interface IdProvider {

        int nextId();

        void reset();
    }

    static class RandomIdProvider implements IdProvider {

        private Random random;
        private final int seed;
        private final int bounds;

        public RandomIdProvider(int seed, int bounds) {
            this.seed = seed;
            this.random = new Random(seed);
            this.bounds = bounds;
        }

        @Override
        public int nextId() {
            return random.nextInt(bounds);
        }

        public void reset() {
            random = new Random(seed);
        }

    }

    static class MonatomicIdProvider implements IdProvider {

        private final long initialValue;
        private final AtomicLong atomicLong;

        public MonatomicIdProvider(long initialValue) {
            this.initialValue = initialValue;
            this.atomicLong = new AtomicLong(initialValue);
        }

        @Override
        public int nextId() {
            return (int) atomicLong.getAndIncrement();
        }

        public void reset() {
            atomicLong.set(initialValue);
        }

    }

//    @Test
//    public void testEnvWithWAL() throws Exception {
//        IdProvider idProvider = new RandomIdProvider(12345, 100_000_000);
//
//        File root = Files.createTempDir();
//        System.out.println("Created root");
//        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
//        LabHeapPressure labHeapPressure = new LabHeapPressure(LABEnvironment.buildLABHeapSchedulerThreadPool(1), 1024 * 1024 * 10, 1024 * 1024 * 10,
//            new AtomicLong());
//        LABEnvironment env = new LABEnvironment(LABEnvironment.buildLABSchedulerThreadPool(1),
//            LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1),
//            "wal",
//            1024 * 1024 * 10,
//            10,
//            1024 * 1024 * 10, root,
//            labHeapPressure, 4, 8, leapsCache);
//
//        ValueIndexConfig valueIndexConfig = new ValueIndexConfig("foo", 4096, 1024 * 1024 * 10, -1, -1, -1,
//            NoOpFormatTransformerProvider.NAME, LABRawhide.NAME, MemoryRawEntryFormat.NAME);
//
//        System.out.println("Created env");
//
//        ValueIndex index = env.open(valueIndexConfig);
//        System.out.println("Open env");
//        index(index, "foo", idProvider, 10, 10, true, false);
//        test(index, "foo", idProvider, 10, 10);
//        System.out.println("Indexed");
//
//        LabWAL labWAL = env.getLabWAL();
//        Assert.assertEquals(labWAL.oldWALCount(), 0);
//        Assert.assertEquals(labWAL.activeWALId(), 10);
//
//        env.close();
//        env.shutdown();
//        System.out.println("Shutdown");
//
//    }
}
