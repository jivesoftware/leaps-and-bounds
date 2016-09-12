package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.LABRawhide;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABRawhideNGTest {

    @Test
    public void rawEntryTest() throws IOException {
        LABRawhide rawhide = LABRawhide.SINGLETON;
        BolBuffer rawEntry = rawhide.toRawEntry(UIO.longBytes(17), 1234, false, 687, UIO.longBytes(45), new BolBuffer());

        Assert.assertEquals(1234, rawhide.timestamp(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry));
        Assert.assertEquals(687, rawhide.version(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry));

    }

    @Test
    public void bbCompareTest() {
        LABRawhide rawhide = LABRawhide.SINGLETON;
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(1), 0, 8, UIO.longBytes(1), 0, 8), 0);
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(0), 0, 8, UIO.longBytes(1), 0, 8), -1);
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(1), 0, 8, UIO.longBytes(0), 0, 8), 1);

        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return rawhide.compareBB(UIO.longBytes(o1), 0, 8, UIO.longBytes(o2), 0, 8);
            }
        });

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {
            Assert.assertEquals((long) sort[i], sorted[i]);
        }
    }

    @Test
    public void compareKeyTest() throws IOException {

        LABRawhide rawhide = LABRawhide.SINGLETON;
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            ByteBuffer.wrap(toRawEntry(1).copy()),
            ByteBuffer.wrap(UIO.longBytes(1))), 0);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            ByteBuffer.wrap(toRawEntry(0).copy()),
            ByteBuffer.wrap(UIO.longBytes(1))), -1);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            ByteBuffer.wrap(toRawEntry(1).copy()),
            ByteBuffer.wrap(UIO.longBytes(0))), 1);

        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                try {
                    return rawhide.compareKey(FormatTransformer.NO_OP,
                        FormatTransformer.NO_OP,
                        ByteBuffer.wrap(toRawEntry(o1).copy()),
                        ByteBuffer.wrap(UIO.longBytes(o2)));
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        });

        System.out.println("hmm:" + Arrays.toString(sort));

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {
            Assert.assertEquals((long) sort[i], sorted[i]);
        }
    }

    private BolBuffer toRawEntry(long key) throws IOException {
        return LABRawhide.SINGLETON.toRawEntry(UIO.longBytes(key), 1234, false, 687, UIO.longBytes(45), new BolBuffer());
    }

    @Test
    public void compareKey2Test() throws IOException {
        LABRawhide rawhide = LABRawhide.SINGLETON;
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            ByteBuffer.wrap(toRawEntry(1).copy()),
            FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            ByteBuffer.wrap(toRawEntry(1).copy())), 0);

        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            ByteBuffer.wrap(toRawEntry(0).copy()),
            FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            ByteBuffer.wrap(toRawEntry(1).copy())), -1);

        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            ByteBuffer.wrap(toRawEntry(1).copy()),
            FormatTransformer.NO_OP, FormatTransformer.NO_OP,
            ByteBuffer.wrap(toRawEntry(0).copy())), 1);

        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                try {
                    return rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(toRawEntry(o1).copy()),
                        FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(toRawEntry(o2).copy()));
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        });

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {
            Assert.assertEquals((long) sort[i], sorted[i]);
        }
    }

}
