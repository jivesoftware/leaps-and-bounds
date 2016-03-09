package com.jivesoftware.os.lab;

import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import junit.framework.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABValidationNGTest {

    @Test(enabled = true, invocationCount = 10, singleThreaded = true)
    public void testConcurrencyMethod() throws Exception {

        ExecutorService compact = Executors.newFixedThreadPool(2,
            new ThreadFactoryBuilder().setNameFormat("lab-compact-%d").build());

        ExecutorService destroy = Executors.newFixedThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("lab-destroy-%d").build());

        File root = Files.createTempDir();
        int entriesBetweenLeaps = 2;
        int maxUpdatesBetweenMerges = 10;
        LAB lab = new LAB(new LABValueMerger(), compact, destroy, root, "lab", true, entriesBetweenLeaps, maxUpdatesBetweenMerges, 4, 8, 128, 0, 0, 2);

        validationTest(lab);

        lab.close();
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
                    for (int c = 0; c < commitCount; c++) {
                        lab.append((ValueStream stream) -> {
                            for (int b = 0; b < batchSize; b++) {
                                count.incrementAndGet();
                                stream.stream(UIO.longBytes(nextId.incrementAndGet()),
                                    System.currentTimeMillis(),
                                    false,
                                    version.incrementAndGet(),
                                    UIO.longBytes(value.incrementAndGet()));
                            }
                            return true;
                        });
                        lab.commit(fsync);
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

//        for (Future f : writerFutures) {
//            f.get();
//        }
//
//        boolean balls = false;
//        do {
//            balls = false;
//            long mid = nextId.get();
//            Set<Long> scanFound = new HashSet<>();
//            for (int i = 1; i <= mid; i++) {
//                lab.get(UIO.longBytes(i), (key, timestamp, tombstoned, version1, value1) -> {
//                    if (value1 != null) {
//                        scanFound.add(UIO.bytesLong(key));
//                    }
//                    return true;
//
//                });
//
//                lab.rowScan((key, timestamp, tombstoned, version1, value1) -> {
//                    if (value1 != null) {
//                        scanFound.add(UIO.bytesLong(key));
//                    }
//                    return true;
//                });
//            }
//
//            if (mid != scanFound.size()) {
//                balls = true;
//                long[] last = new long[1];
//                lab.rowScan((key, timestamp, tombstoned, version1, value1) -> {
//                    long l = UIO.bytesLong(key);
//                    if (l - 1 != last[0]) {
//                        System.out.println("Fuck:" + l);
//                    }
//                    last[0] = l;
//                    return true;
//                });
//
//                List<Long> missing = new ArrayList<>();
//                for (long i = 1; i <= mid; i++) {
//                    if (!scanFound.contains(i)) {
//                        missing.add(i);
//                    }
//                }
//                System.out.println("SCAN FAILED: " + scanFound.size() + "  vs " + mid + " " + missing + " " + missing.size());
//                //org.testng.Assert.fail();
//            }
//            if (balls) {
//                Thread.sleep(1000);
//            }
//        } while (balls);
//
//        if (2 + 2 == 4) {
//            return;
//        }
//        System.out.println("------------------------- WRITERS ARE DONE -----------------------");

        AtomicLong passed = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        List<String> log = new ArrayList<>();

        List<Future> readerFutures = new ArrayList<>();
        for (int r = 0; r < readerCount; r++) {
            int readerId = r;
            readerFutures.add(readers.submit(() -> {
                try {
                    int overRun = 10;
                    while (running.get() > 0 || overRun > 0) {
                        long maxId = nextId.get();
                        Set<Long> found = new HashSet<>();
                        for (int i = 0; i < maxId; i++) {
                            lab.get(UIO.longBytes(i), (key, timestamp, tombstoned, version1, value1) -> {
                                hits.incrementAndGet();
                                found.add(UIO.bytesLong(key));
                                return true;
                            });
                        }

//                        Set<Long> scanFound = new HashSet<>();
//                        for (int i = 0; i < maxId; i++) {
//                            lab.rowScan((byte[] key, long timestamp, boolean tombstoned, long version1, byte[] payload) -> {
//                                scanFound.add(UIO.bytesLong(key));
//                                return true;
//                            });
//                        }
//                        if (maxId != scanFound.size()) {
//                            failed.incrementAndGet();
//                            System.out.println("SCAN FAILED: " + scanFound.size() + "  vs " + maxId);
//                            List<Long> missing = new ArrayList<>();
//                            for (long i = 0; i < maxId; i++) {
//                                if (!scanFound.contains(i)) {
//                                    missing.add(i);
//                                }
//
//                            }
//                            log.add("SCAN FAILED: " + found.size() + "  vs " + maxId + " missing=" + missing);
//                        }
                        if (maxId == found.size()) {
                            passed.incrementAndGet();
                            System.out.println("PASSED: " + found.size() + "  vs " + maxId);
                            log.add("PASSED: " + found.size() + "  vs " + maxId);
                        } else {
                            failed.incrementAndGet();
                            List<Long> missing = new ArrayList<>();
                            for (long i = 0; i < maxId; i++) {
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

    //Thread.sleep(Integer.MAX_VALUE);
}
