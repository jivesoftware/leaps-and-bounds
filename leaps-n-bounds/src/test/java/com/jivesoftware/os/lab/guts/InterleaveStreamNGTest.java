package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.LABConcurrentSkipListMap;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LabHeapPressure;
import com.jivesoftware.os.lab.StripingBolBufferLocks;
import com.jivesoftware.os.lab.TestUtils;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.guts.allocators.LABAppendOnlyAllocator;
import com.jivesoftware.os.lab.guts.allocators.LABConcurrentSkipListMemory;
import com.jivesoftware.os.lab.guts.allocators.LABIndexableMemory;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.guts.api.Scanner;
import com.jivesoftware.os.lab.guts.api.Scanner.Next;
import com.jivesoftware.os.lab.io.api.UIO;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class InterleaveStreamNGTest {

    @Test
    public void testNext() throws Exception {

        InterleaveStream ips = new InterleaveStream(new ReadIndex[]{
            sequenceIndex(new long[]{1, 2, 3, 4, 5}, new long[]{3, 3, 3, 3, 3})
        }, null, null, new SimpleRawhide());

        List<Expected> expected = new ArrayList<>();
        expected.add(new Expected(1, 3));
        expected.add(new Expected(2, 3));
        expected.add(new Expected(3, 3));
        expected.add(new Expected(4, 3));
        expected.add(new Expected(5, 3));

        assertExpected(ips, expected);
        ips.close();
    }

    @Test
    public void testNext1() throws Exception {

        InterleaveStream ips = new InterleaveStream(new ReadIndex[]{
            sequenceIndex(new long[]{1, 2, 3, 4, 5}, new long[]{3, 3, 3, 3, 3}),
            sequenceIndex(new long[]{1, 2, 3, 4, 5}, new long[]{2, 2, 2, 2, 2}),
            sequenceIndex(new long[]{1, 2, 3, 4, 5}, new long[]{1, 1, 1, 1, 1})
        }, null, null, new SimpleRawhide());

        List<Expected> expected = new ArrayList<>();
        expected.add(new Expected(1, 3));
        expected.add(new Expected(2, 3));
        expected.add(new Expected(3, 3));
        expected.add(new Expected(4, 3));
        expected.add(new Expected(5, 3));

        assertExpected(ips, expected);
        ips.close();
    }

    @Test
    public void testNext2() throws Exception {

        InterleaveStream ips = new InterleaveStream(new ReadIndex[]{
            sequenceIndex(new long[]{10, 21, 29, 41, 50}, new long[]{1, 0, 0, 0, 1}),
            sequenceIndex(new long[]{10, 21, 29, 40, 50}, new long[]{0, 0, 0, 1, 0}),
            sequenceIndex(new long[]{10, 20, 30, 39, 50}, new long[]{0, 1, 1, 0, 0})
        }, null, null, new SimpleRawhide());

        List<Expected> expected = new ArrayList<>();
        expected.add(new Expected(10, 1));
        expected.add(new Expected(20, 1));
        expected.add(new Expected(21, 0));
        expected.add(new Expected(29, 0));
        expected.add(new Expected(30, 1));
        expected.add(new Expected(39, 0));
        expected.add(new Expected(40, 1));
        expected.add(new Expected(41, 0));
        expected.add(new Expected(50, 1));

        assertExpected(ips, expected);
        ips.close();

    }

    private ReadIndex sequenceIndex(long[] keys, long[] values) {
        return new ReadIndex() {
            @Override
            public void release() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public GetRaw get() throws Exception {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Scanner rangeScan(byte[] from, byte[] to) throws Exception {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Scanner rowScan() throws Exception {
                return nextEntrySequence(keys, values);
            }

            @Override
            public void close() throws Exception {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public long count() throws Exception {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public boolean isEmpty() throws Exception {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
    }

    @Test
    public void testNext3() throws Exception {

        int count = 10;
        int step = 100;
        int indexes = 4;

        Random rand = new Random(123);
        ExecutorService destroy = Executors.newSingleThreadExecutor();
        SimpleRawhide rawhide = new SimpleRawhide();

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(rawhide.getKeyComparator());

        LabHeapPressure labHeapPressure = new LabHeapPressure(LABEnvironment.buildLABHeapSchedulerThreadPool(1), "default", -1, -1, new AtomicLong());
        RawMemoryIndex[] memoryIndexes = new RawMemoryIndex[indexes];
        ReadIndex[] reorderIndexReaders = new ReadIndex[indexes];
        ReadIndex[] readerIndexs = new ReadIndex[indexes];
        BolBuffer keyBuffer = new BolBuffer();
        try {
            for (int wi = 0; wi < indexes; wi++) {

                int i = (indexes - 1) - wi;

                memoryIndexes[i] = new RawMemoryIndex(destroy, labHeapPressure,
                    rawhide,
                    new LABConcurrentSkipListMap(
                        new LABConcurrentSkipListMemory(rawhide,
                            new LABIndexableMemory("memory",
                                new LABAppendOnlyAllocator(30)
                            )
                        ),
                        new StripingBolBufferLocks(1024)
                    ));
                TestUtils.append(rand, memoryIndexes[i], 0, step, count, desired, keyBuffer);
                System.out.println("Index " + i);

                readerIndexs[wi] = memoryIndexes[i].acquireReader();
                Scanner nextRawEntry = readerIndexs[wi].rowScan();
                while (nextRawEntry.next((readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    System.out.println(SimpleRawhide.toString(rawEntry));
                    return true;
                }) == Scanner.Next.more) {
                }
                System.out.println("\n");

                reorderIndexReaders[i] = readerIndexs[wi];
            }

            InterleaveStream ips = new InterleaveStream(reorderIndexReaders, null, null, rawhide);

            List<Expected> expected = new ArrayList<>();
            System.out.println("Expected:");
            for (Map.Entry<byte[], byte[]> entry : desired.entrySet()) {
                long key = UIO.bytesLong(entry.getKey());
                long value = SimpleRawhide.value(entry.getValue());
                expected.add(new Expected(key, value));
                System.out.println(key + " timestamp:" + value);
            }
            System.out.println("\n");

            assertExpected(ips, expected);
        } finally {
            for (ReadIndex readerIndex : readerIndexs) {
                if (readerIndex != null) {
                    readerIndex.release();
                }
            }
        }
    }

    private void assertExpected(InterleaveStream ips, List<Expected> expected) throws Exception {
        while (ips.next((readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
            Expected expect = expected.remove(0);
            long key = SimpleRawhide.key(rawEntry);
            long value = SimpleRawhide.value(rawEntry);

            System.out.println("key:" + key + " vs " + expect.key + " value:" + value + " vs " + expect.value);
            Assert.assertEquals(key, expect.key, "unexpected key ");
            Assert.assertEquals(value, expect.value, key + " unexpected value ");
            return true;
        }) == Scanner.Next.more) {
        }
        Assert.assertTrue(expected.isEmpty());
    }


    /*

     */
    static private class Expected {

        long key;
        long value;

        private Expected(long key, long value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Expected{" + "key=" + key + ", value=" + value + '}';
        }

    }

    public Scanner nextEntrySequence(long[] keys, long[] values) {
        int[] index = {0};

        return new Scanner() {
            @Override
            public Next next(RawEntryStream stream) throws Exception {
                if (index[0] < keys.length) {
                    byte[] rawEntry = SimpleRawhide.rawEntry(keys[index[0]], values[index[0]]);
                    if (!stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(rawEntry))) {
                        return Next.stopped;
                    }
                }
                index[0]++;
                return index[0] <= keys.length ? Next.more : Next.eos;
            }

            @Override
            public void close() {
            }
        };
    }

}
