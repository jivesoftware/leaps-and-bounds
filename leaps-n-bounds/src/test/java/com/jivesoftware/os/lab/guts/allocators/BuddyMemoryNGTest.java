package com.jivesoftware.os.lab.guts.allocators;

import com.jivesoftware.os.lab.guts.allocators.LABCASBuddyAllocator;
import com.google.common.collect.Lists;
import com.jivesoftware.os.lab.io.api.UIO;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class BuddyMemoryNGTest {

//    @Test(invocationCount = 1)
//    public void testBla() throws Exception {
//        System.out.println(1 << 0);
//        System.out.println(1 << 1);
//        System.out.println(1 << 2);
//        System.out.println(1 << 3);
//        System.out.println(1 << 4);
//        System.out.println(1 << 5);
//
//        int totalSize = (1 << 8);
//        System.out.println("totalSize:" + totalSize);
//        for (int l = 0; l < 8; l++) {
//            System.out.println("Level:" + l);
//
//            int levelBlockSize = 1 << (8 - l);
//            int numBlocksInLevel = totalSize / levelBlockSize;
//            for (int p = 0; p < totalSize; p += levelBlockSize) {
//
//                int levelIndex = (numBlocksInLevel + (p / (totalSize / (1 << l)))) - 1;
//
//                System.out.print(levelIndex + ", ");
//            }
//            System.out.println("\n\n");
//        }
//    }

    @Test(invocationCount = 1)
    public void testBytes() throws Exception {

        byte minPowerSize = 4;
        byte maxPowerSize = 16;
        int count = 100_000;
        boolean validate = true;

        int maxAllocatePower = 4;

        Map<Long, byte[]> allocated = new ConcurrentHashMap<>();
        List<Long> arrayOfAllocated = Lists.newArrayListWithCapacity(count);

        long[] timeInGC = {0};
        LABCASBuddyAllocator[] buddyMemory = new LABCASBuddyAllocator[1];
        Callable<Void> requestGC = () -> {
            //System.out.println("> GC requested " + arrayOfAllocated.size());
            //buddyMemory[0].dump();

            for (Long a : arrayOfAllocated) {
                if (validate) {
                    byte[] expected = allocated.get(a);
                    byte[] found = buddyMemory[0].bytes(a);
                    try {
                        Assert.assertEquals(expected, found, "address:" + a + " " + Arrays.toString(expected) + " vs " + Arrays.toString(found));
                    } catch (Error e) {
                        buddyMemory[0].dump();
                        throw e;
                    }
                }
                long start = System.nanoTime();
                buddyMemory[0].release(a);
                timeInGC[0] += System.nanoTime() - start;
            }
            arrayOfAllocated.clear();
            allocated.clear();
            //System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~");
            //buddyMemory[0].dump();
            //System.out.println("< GC completed. ");
            return null;
        };
        buddyMemory[0] = new LABCASBuddyAllocator("buddy", minPowerSize, maxPowerSize, requestGC);
        //buddyMemory[0].dump();

        Random rand = new Random();
        byte[] bytes = new byte[(int) UIO.chunkLength(maxAllocatePower)];
        rand.nextBytes(bytes);

        long elapse = 0;
        for (int i = 0; i < count; i++) {

            int l = 1 + rand.nextInt(bytes.length - 1);
            long start = System.nanoTime();
            //System.out.println(i + " requesting " + l+ " bytes");
            long address = buddyMemory[0].allocate(bytes, 0, l);
            elapse += System.nanoTime() - start;

            arrayOfAllocated.add(address);
            if (validate) {
                allocated.put(address, Arrays.copyOf(bytes, l));
            }

            //buddyMemory[0].dump();
            //System.out.println(i + " allocated address=" + address + "\n");
        }
        System.out.println("Force GC");
        requestGC.call();
        buddyMemory[0].dump();
        System.out.println(
            "Allocated " + count + " in " + (elapse / 1000000) + "millis gc:" + (timeInGC[0] / 1000000) + "millis total:" + ((elapse + timeInGC[0]) / 1000000));

        Runtime.getRuntime().gc();
    }

    @Test(invocationCount = 1)
    public void testConcurrency() throws Exception {
        byte minPowerSize = 4;
        byte maxPowerSize = 16;
        int count = 100_000;
        int numThreads = 2;
        boolean validate = false;

        int maxAllocatePower = 6;

        @SuppressWarnings("unchecked")
        Map<Long, byte[]>[] allocated = new Map[numThreads];
        @SuppressWarnings("unchecked")
        ConcurrentLinkedDeque<Long>[] arrayOfAllocated = new ConcurrentLinkedDeque[numThreads];
        for (int i = 0; i < numThreads; i++) {
            allocated[i] = new ConcurrentHashMap<>();
            arrayOfAllocated[i] = new ConcurrentLinkedDeque<>();
        }
        Map<Thread, Integer> threadToIndex = new HashMap<>();

        long[] timeInGC = {0};
        LABCASBuddyAllocator[] buddyMemory = new LABCASBuddyAllocator[1];
        Callable<Void> requestGC = () -> {
            //System.out.println("> GC requested " + arrayOfAllocated.size());
            //buddyMemory[0].dump();
            //System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\nSTART GC");

            Integer index = threadToIndex.get(Thread.currentThread());
            if (index != null) {
                while (true) {
                    Long a = arrayOfAllocated[index].pollFirst();
                    if (a == null) {
                        break;
                    }
                    if (validate) {
                        byte[] expected = allocated[index].remove(a);
                        byte[] found = buddyMemory[0].bytes(a);
                        try {
                            synchronized (System.out) {
                                Assert.assertEquals(expected, found, "address:" + a + " " + Arrays.toString(expected) + " vs " + Arrays.toString(found));
                            }
                        } catch (Error e) {
                            //buddyMemory[0].dump();
                            throw e;
                        }
                    }
                    long start = System.nanoTime();
//                synchronized (System.out) {
//                    System.out.println(Thread.currentThread().getName() + " RELEASED:" + a);
//                }
                    buddyMemory[0].release(a);
                    timeInGC[0] += System.nanoTime() - start;
                }
            } else {
                System.out.println("Who the hell are you? " + Thread.currentThread().getName());
                for (index = 0; index < numThreads; index++) {
                    while (true) {
                        Long a = arrayOfAllocated[index].pollFirst();
                        if (a == null) {
                            break;
                        }
                        if (validate) {
                            byte[] expected = allocated[index].remove(a);
                            byte[] found = buddyMemory[0].bytes(a);
                            try {
                                synchronized (System.out) {
                                    Assert.assertEquals(expected, found, "address:" + a + " " + Arrays.toString(expected) + " vs " + Arrays.toString(found));
                                }
                            } catch (Error e) {
                                //buddyMemory[0].dump();
                                throw e;
                            }
                        }
                        long start = System.nanoTime();

                        buddyMemory[0].release(a);
                        timeInGC[0] += System.nanoTime() - start;
                    }

                }
            }

            //System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~");
            //buddyMemory[0].dump();
            //System.out.println("< GC completed. ");
            return null;
        };
        buddyMemory[0] = new LABCASBuddyAllocator("buddy", minPowerSize, maxPowerSize, requestGC);
        //buddyMemory[0].dump();
        ExecutorService threads = Executors.newCachedThreadPool();
        List<Future<Object>> futures = Lists.newArrayList();
        for (int t = 0; t < numThreads; t++) {
            int n = t;
            futures.add(threads.submit(() -> {

                threadToIndex.put(Thread.currentThread(), n);

                try {

                    Random rand = new Random();
                    byte[] bytes = new byte[(int) UIO.chunkLength(maxAllocatePower)];
                    rand.nextBytes(bytes);

                    long elapse = 0;
                    for (int i = 0; i < count; i++) {

                        int l = 1 + rand.nextInt(bytes.length - 1);
                        long start = System.nanoTime();
                        //System.out.println(i + " requesting " + l+ " bytes");
                        long address = buddyMemory[0].allocate(bytes, 0, l);
//                        synchronized (System.out) {
//                            System.out.println(Thread.currentThread().getName() + " ALLOCATED:" + address);
//                        }

                        elapse += System.nanoTime() - start;

                        if (validate) {
                            allocated[n].put(address, Arrays.copyOf(bytes, l));
                        }
                        arrayOfAllocated[n].add(address);

                        //buddyMemory[0].dump();
                        //System.out.println(i + " allocated address=" + address + "\n");
                        if (i % 10000 == 0) {
                            System.out.print(n);
                        }
                    }
                    System.out.println(
                        "Allocated " + count + " in " + (elapse / 1000000) + "millis gc:" + (timeInGC[0] / 1000000) + "millis total:" + ((elapse + timeInGC[0]) / 1000000));
                    return null;
                } catch (Throwable e) {
                    e.printStackTrace();
                    System.out.println(Thread.currentThread().getName() + " DIED " + e);
                    System.exit(1);
                    throw e;
                }
            }));

        }

        for (Future<Object> future : futures) {
            future.get();
        }

        System.out.println("Force GC");
        requestGC.call();
        buddyMemory[0].dump();

//        System.out.println(
//            "a:" + BuddyMemory.allocatations.get() + " r:" + BuddyMemory.releases.get() + " s:" + BuddyMemory.split.get() + " m:" + BuddyMemory.merged.get());
        Runtime.getRuntime().gc();
    }

}
