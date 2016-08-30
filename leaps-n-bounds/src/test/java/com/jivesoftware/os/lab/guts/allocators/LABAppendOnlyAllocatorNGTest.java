package com.jivesoftware.os.lab.guts.allocators;

import com.jivesoftware.os.lab.guts.allocators.LABAppendOnlyAllocator;
import com.google.common.collect.Lists;
import com.jivesoftware.os.lab.io.api.UIO;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABAppendOnlyAllocatorNGTest {

    @Test(invocationCount = 1)
    public void testBytes() throws Exception {

        int count = 1_000_000;
        boolean validate = true;

        int maxAllocatePower = 4;

        Map<Long, byte[]> allocated = new ConcurrentHashMap<>();
        List<Long> arrayOfAllocated = Lists.newArrayListWithCapacity(count);

        long[] timeInGC = {0};
        LABAppendOnlyAllocator[] allocator = new LABAppendOnlyAllocator[1];
        Callable<Void> requestGC = () -> {
            //System.out.println("Gc");
            for (Long a : arrayOfAllocated) {
                if (validate) {
                    byte[] expected = allocated.get(a);
                    byte[] found = allocator[0].bytes(a);
                    try {
                        Assert.assertEquals(expected, found, "address:" + a + " " + Arrays.toString(expected) + " vs " + Arrays.toString(found));
                    } catch (Error e) {
                        //allocator[0].dump();
                        throw e;
                    }
                }
                long start = System.nanoTime();
                allocator[0].release(a);
                timeInGC[0] += System.nanoTime() - start;
            }
            arrayOfAllocated.clear();
            allocated.clear();
            allocator[0].freeAll();

            return null;
        };
        allocator[0] = new LABAppendOnlyAllocator(1024 * 1024 * 2, requestGC);

        Random rand = new Random();
        byte[] bytes = new byte[(int) UIO.chunkLength(maxAllocatePower)];
        rand.nextBytes(bytes);

        long elapse = 0;
        for (int i = 0; i < count; i++) {

            int l = 1 + rand.nextInt(bytes.length - 1);
            long start = System.nanoTime();
            long address = allocator[0].allocate(bytes, 0, l);
            elapse += System.nanoTime() - start;

            arrayOfAllocated.add(address);
            if (validate) {
                allocated.put(address, Arrays.copyOf(bytes, l));
            }

        }
        System.out.println("Force GC");
        requestGC.call();
        System.out.println(
            "Allocated " + count + " in " + (elapse / 1000000) + "millis gc:" + (timeInGC[0] / 1000000) + "millis total:" + ((elapse + timeInGC[0]) / 1000000));

        Runtime.getRuntime().gc();
    }

}
