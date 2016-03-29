package com.jivesoftware.os.lab.guts;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.NextRawEntry.Next;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.io.api.UIO;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class InterleaveStreamNGTest {

    @Test
    public void testNext() throws Exception {

        InterleaveStream ips = new InterleaveStream(new NextRawEntry[]{
            nextEntrySequence(new long[]{1, 2, 3, 4, 5}, new long[]{3, 3, 3, 3, 3})
        });

        List<Expected> expected = new ArrayList<>();
        expected.add(new Expected(1, 3));
        expected.add(new Expected(2, 3));
        expected.add(new Expected(3, 3));
        expected.add(new Expected(4, 3));
        expected.add(new Expected(5, 3));

        assertExpected(ips, expected);
    }

    private void assertExpected(InterleaveStream ips, List<Expected> expected) throws Exception {
        while (ips.next((rawEntry, offset, length) -> {
            Expected expect = expected.remove(0);
            System.out.println("key:" + SimpleRawEntryMarshaller.key(rawEntry) + " vs" + expect.key + " value:" + SimpleRawEntryMarshaller.value(rawEntry) + " vs " + expect.value);
            Assert.assertEquals(SimpleRawEntryMarshaller.key(rawEntry), expect.key);
            Assert.assertEquals(SimpleRawEntryMarshaller.value(rawEntry), expect.value);
            return true;
        }) == NextRawEntry.Next.more);
        Assert.assertTrue(expected.isEmpty());
    }

    @Test
    public void testNext1() throws Exception {

        InterleaveStream ips = new InterleaveStream(new NextRawEntry[]{
            nextEntrySequence(new long[]{1, 2, 3, 4, 5}, new long[]{3, 3, 3, 3, 3}),
            nextEntrySequence(new long[]{1, 2, 3, 4, 5}, new long[]{2, 2, 2, 2, 2}),
            nextEntrySequence(new long[]{1, 2, 3, 4, 5}, new long[]{1, 1, 1, 1, 1})
        });

        List<Expected> expected = new ArrayList<>();
        expected.add(new Expected(1, 3));
        expected.add(new Expected(2, 3));
        expected.add(new Expected(3, 3));
        expected.add(new Expected(4, 3));
        expected.add(new Expected(5, 3));

        assertExpected(ips, expected);

    }

    @Test
    public void testNext2() throws Exception {

        InterleaveStream ips = new InterleaveStream(new NextRawEntry[]{
            nextEntrySequence(new long[]{10, 21, 29, 41, 50}, new long[]{1, 0, 0, 0, 1}),
            nextEntrySequence(new long[]{10, 21, 29, 40, 50}, new long[]{0, 0, 0, 1, 0}),
            nextEntrySequence(new long[]{10, 20, 30, 39, 50}, new long[]{0, 1, 1, 0, 0})
        });

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

    }

    @Test
    public void testNext3() throws Exception {

        int count = 10;
        int step = 100;
        int indexes = 4;

        Random rand = new Random();
        ExecutorService destroy = Executors.newSingleThreadExecutor();

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        RawMemoryIndex[] memoryIndexes = new RawMemoryIndex[indexes];
        NextRawEntry[] nextRawEntrys = new NextRawEntry[indexes];
        ReadIndex[] readerIndexs = new ReadIndex[indexes];
        try {
            for (int wi = 0; wi < indexes; wi++) {

                int i = (indexes - 1) - wi;

                memoryIndexes[i] = new RawMemoryIndex(destroy, new SimpleRawEntryMarshaller());
                IndexTestUtils.append(rand, memoryIndexes[i], 0, step, count, desired);
                System.out.println("Index " + i);

                readerIndexs[wi] = memoryIndexes[i].acquireReader();
                NextRawEntry nextRawEntry = readerIndexs[wi].rowScan();
                while (nextRawEntry.next((rawEntry, offset, length) -> {
                    System.out.println(SimpleRawEntryMarshaller.toString(rawEntry));
                    return true;
                }) == NextRawEntry.Next.more);
                System.out.println("\n");

                nextRawEntrys[i] = readerIndexs[wi].rowScan();
            }

            InterleaveStream ips = new InterleaveStream(nextRawEntrys);

            List<Expected> expected = new ArrayList<>();
            System.out.println("Expected:");
            for (Map.Entry<byte[], byte[]> entry : desired.entrySet()) {
                expected.add(new Expected(UIO.bytesLong(entry.getKey()), SimpleRawEntryMarshaller.value(entry.getValue())));
                System.out.println(UIO.bytesLong(entry.getKey()) + " timestamp:" + SimpleRawEntryMarshaller.value(entry.getValue()));
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

    /*

     */
    static private class Expected {

        long key;
        long value;

        private Expected(long key, long value) {
            this.key = key;
            this.value = value;
        }

    }

    public NextRawEntry nextEntrySequence(long[] keys, long[] values) {
        int[] index = {0};
        return (stream) -> {
            if (index[0] < keys.length) {
                byte[] rawEntry = SimpleRawEntryMarshaller.rawEntry(keys[index[0]], values[index[0]]);
                if (!stream.stream(rawEntry, 0, rawEntry.length)) {
                    return Next.stopped;
                }
            }
            index[0]++;
            return index[0] <= keys.length ? Next.more : Next.eos;
        };
    }

}
