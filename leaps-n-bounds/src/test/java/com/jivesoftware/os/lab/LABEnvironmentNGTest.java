package com.jivesoftware.os.lab;

import com.google.common.io.Files;
import com.jivesoftware.os.lab.api.NextValue;
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

        File root = Files.createTempDir();
        LABEnvironment env = new LABEnvironment(root, new LABValueMerger(), false, 4, 8);

        ValueIndex index = env.open("foo", 1000);
        indexTest(index);

        env = new LABEnvironment(root, new LABValueMerger(), true, 4, 8);

        index = env.open("foo", 1000);
        indexTest(index);

        env.shutdown();

        env = new LABEnvironment(root, new LABValueMerger(), true, 4, 8);
        env.rename("foo", "bar");
        index = env.open("bar", 1000);

        indexTest(index);

        env.shutdown();
        env = new LABEnvironment(root, new LABValueMerger(), true, 4, 8);
        env.remove("bar");

    }

    @Test
    public void testEnvWithMemMap() throws Exception {

        File root = Files.createTempDir();
        LABEnvironment env = new LABEnvironment(root, new LABValueMerger(), true, 4, 8);

        ValueIndex index = env.open("foo", 1000);
        indexTest(index);

        env.shutdown();

        env = new LABEnvironment(root, new LABValueMerger(), true, 4, 8);

        index = env.open("foo", 1000);
        indexTest(index);

        env.shutdown();

    }

    private void indexTest(ValueIndex index) throws Exception, InterruptedException {

        AtomicLong version = new AtomicLong();
        AtomicLong pointer = new AtomicLong();
        AtomicLong count = new AtomicLong();

        int totalCardinality = 100_000_000;
        int commitCount = 34;
        int batchCount = 3_000;
        int getCount = 0;
        boolean fsync = true;

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
                        UIO.longBytes(pointer.incrementAndGet()));
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
                index.get(UIO.longBytes(rand.nextInt(1_000_000)), (NextValue nextPointer) -> {
                    nextPointer.next((key, timestamp, tombstoned, version1, pointer1) -> {
                        hits.incrementAndGet();
                        return true;
                    });
                    return null;
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

        index.rowScan((nextValue) -> {
            nextValue.next((byte[] key, long timestamp, boolean tombstoned, long version1, byte[] payload) -> {
                System.out.println(Arrays.toString(key) + " " + timestamp + " " + tombstoned + " " + version1 + " " + Arrays.toString(payload));
                return true;
            });
            return null;
        });

    }

}
