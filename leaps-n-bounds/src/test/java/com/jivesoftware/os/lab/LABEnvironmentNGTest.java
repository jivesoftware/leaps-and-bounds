package com.jivesoftware.os.lab;

import com.google.common.io.Files;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABEnvironmentNGTest {

    @Test
    public void testEnv() throws Exception {

        File root = null;
        try {
            root = Files.createTempDir();
            System.out.println("root" + root.getAbsolutePath());
            LABEnvironment env = new LABEnvironment(LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1), root,
                new LABRawEntryMarshaller(), false, 4, 8, 8);

            ValueIndex index = env.open("foo", 4096, 1000, -1, -1, -1);
            indexTest(index);

            env = new LABEnvironment(LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1), root, new LABRawEntryMarshaller(),
                true, 4, 8, 8);

            index = env.open("foo", 4096, 1000, -1, -1, -1);
            indexTest(index);

            env.shutdown();

            env = new LABEnvironment(LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1), root, new LABRawEntryMarshaller(),
                true, 4, 8, 8);
            env.rename("foo", "bar");
            index = env.open("bar", 4096, 1000, -1, -1, -1);

            indexTest(index);

            env.shutdown();
            env = new LABEnvironment(LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1), root, new LABRawEntryMarshaller(),
                true, 4, 8, 8);
            env.remove("bar");
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

        File root = Files.createTempDir();
        System.out.println("Created root");
        LABEnvironment env = new LABEnvironment(LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1), root,
            new LABRawEntryMarshaller(), true, 4, 8, 8);
        System.out.println("Created env");

        ValueIndex index = env.open("foo", 4096, 1000, -1, -1, -1);
        System.out.println("Open env");
        indexTest(index);
        System.out.println("Indexed");

        env.shutdown();
        System.out.println("Shutdown");

        env = new LABEnvironment(LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1),
            root, new LABRawEntryMarshaller(), true, 4,
            8, 8);
        System.out.println("Recreate env");

        index = env.open("foo", 4096, 1000, -1, -1, -1);
        System.out.println("Re-open env");
        indexTest(index);
        System.out.println("Re-indexed");
        env.shutdown();
        System.out.println("Re-shutdown");

    }

    private void indexTest(ValueIndex index) throws Exception, InterruptedException {

        AtomicLong version = new AtomicLong();
        AtomicLong value = new AtomicLong();
        AtomicLong count = new AtomicLong();

        int totalCardinality = 100_000_000;
        int commitCount = 34;
        int batchCount = 3_000;
        int getCount = 0;
       
        long mainStart = System.currentTimeMillis();
        Random rand = new Random(12345);
        for (int c = 0; c < commitCount; c++) {
            long start = System.currentTimeMillis();
            index.append((ValueStream stream) -> {
                for (int i = 0; i < batchCount; i++) {
                    count.incrementAndGet();
                    stream.stream(UIO.longBytes(rand.nextInt(totalCardinality)),
                        System.currentTimeMillis(),
                        rand.nextBoolean(),
                        version.incrementAndGet(),
                        UIO.longBytes(value.incrementAndGet()));
                }
                return true;
            });

            System.out.println("Append Elapse:" + (System.currentTimeMillis() - start));
            start = System.currentTimeMillis();
            index.commit(true);
            System.out.println("Commit Elapse:" + (System.currentTimeMillis() - start));
            start = System.currentTimeMillis();

            AtomicLong hits = new AtomicLong();
            for (int i = 0; i < getCount; i++) {
                index.get(UIO.longBytes(rand.nextInt(1_000_000)), (key, timestamp, tombstoned, version1, value1) -> {
                    hits.incrementAndGet();
                    return true;
                });
            }
            System.out.println("Get (" + hits.get() + ") Elapse:" + (System.currentTimeMillis() - start));

            double rate = (double) count.get() * 1000 / ((System.currentTimeMillis() - mainStart));

            System.out.println("Count:" + count.get() + " " + rate);
            System.out.println("-----------------------------------");

        }

        System.out.println("Total Time:" + (System.currentTimeMillis() - mainStart));

        System.out.println("Index Count:" + index.count());
        System.out.println("Is empty:" + index.isEmpty());

        index.rowScan((byte[] key, long timestamp, boolean tombstoned, long version1, byte[] payload) -> {
            System.out.println(Arrays.toString(key) + " " + timestamp + " " + tombstoned + " " + version1 + " " + Arrays.toString(payload));
            return true;
        });

    }

}
