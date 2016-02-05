package com.jivesoftware.os.lab;

import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.lab.api.NextValue;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
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
        LABEnvironment env = new LABEnvironment(root, new LABValueMerger(), false, 4, 8);

        concurentTest(env);
    }

    @Test(enabled = true)
    public void testConcurrencyWithMemMapMethod() throws Exception {

        File root = Files.createTempDir();
        LABEnvironment env = new LABEnvironment(root, new LABValueMerger(), true, 4, 8);

        concurentTest(env);
    }

    private void concurentTest(LABEnvironment env) throws InterruptedException, Exception, ExecutionException {
        int writerCount = 16;
        int readerCount = 16;

        AtomicLong hits = new AtomicLong();
        AtomicLong version = new AtomicLong();
        AtomicLong pointer = new AtomicLong();
        AtomicLong count = new AtomicLong();

        int totalCardinality = 100_000_000;
        int commitCount = 100;
        int batchCount = 100;
        boolean fsync = true;

        ExecutorService writers = Executors.newFixedThreadPool(writerCount, new ThreadFactoryBuilder().setNameFormat("writers-%d").build());
        ExecutorService readers = Executors.newFixedThreadPool(readerCount, new ThreadFactoryBuilder().setNameFormat("readers-%d").build());

        Random rand = new Random(12345);
        ValueIndex index = env.open("foo", 1000);
        AtomicLong running = new AtomicLong();
        List<Future> writerFutures = new ArrayList<>();
        for (int i = 0; i < writerCount; i++) {
            running.incrementAndGet();
            writerFutures.add(writers.submit(() -> {
                try {
                    for (int c = 0; c < commitCount; c++) {
                        index.append((ValueStream stream) -> {
                            for (int b = 0; b < batchCount; b++) {
                                count.incrementAndGet();
                                stream.stream(UIO.longBytes(rand.nextInt(totalCardinality)),
                                    System.currentTimeMillis(),
                                    rand.nextBoolean(),
                                    version.incrementAndGet(),
                                    UIO.longBytes(pointer.incrementAndGet()));
                            }
                            return true;
                        });
                        index.commit(fsync);
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
                        index.get(UIO.longBytes(rand.nextInt(1_000_000)), (NextValue nextPointer) -> {
                            nextPointer.next((key, timestamp, tombstoned, version1, pointer1) -> {
                                hits.incrementAndGet();
                                return true;
                            });
                            return null;
                        });
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

        env.shutdown();
        System.out.println("ALL DONE");

        writers.shutdownNow();
        readers.shutdownNow();

        //Thread.sleep(Integer.MAX_VALUE);
    }

}
