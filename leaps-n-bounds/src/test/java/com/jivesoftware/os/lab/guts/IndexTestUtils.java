package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.RawAppendableIndex;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.io.api.UIO;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import junit.framework.Assert;

import static com.jivesoftware.os.lab.guts.SimpleRawhide.key;
import static com.jivesoftware.os.lab.guts.SimpleRawhide.rawEntry;
import static com.jivesoftware.os.lab.guts.SimpleRawhide.value;

/**
 * @author jonathan.colt
 */
public class IndexTestUtils {

    private static OrderIdProvider timeProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));

    static public long append(Random rand,
        RawAppendableIndex index,
        long start,
        int step,
        int count,
        ConcurrentSkipListMap<byte[], byte[]> desired) throws Exception {

        long[] lastKey = new long[1];
        index.append((stream) -> {
            long k = start;
            for (int i = 0; i < count; i++) {
                k += 1 + rand.nextInt(step);

                byte[] key = UIO.longBytes(k);
                long time = timeProvider.nextId();

                long specialK = k;
                if (desired != null) {
                    desired.compute(key, (t, u) -> {
                        if (u == null) {
                            return rawEntry(specialK, time);
                        } else {
                            return value(u) > time ? u : rawEntry(specialK, time);
                        }
                    });
                }
                byte[] rawEntry = rawEntry(k, time);
                if (!stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry, 0, rawEntry.length)) {
                    break;
                }
            }
            lastKey[0] = k;
            return true;
        });
        return lastKey[0];
    }

    static public void assertions(CompactableIndexes indexes,
        int count, int step,
        ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {

        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];

        SimpleRawhide rawhide = new SimpleRawhide();
        indexes.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            NextRawEntry rowScan = IndexUtil.rowScan(acquired, rawhide);
            RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {
                System.out.println("scanned:" + UIO.bytesLong(keys.get(index[0])) + " " + key(rawEntry));
                Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), key(rawEntry));
                index[0]++;
                return true;
            };
            while (rowScan.next(stream) == NextRawEntry.Next.more) ;
            System.out.println("rowScan PASSED");
            return true;
        }, true);

        indexes.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            for (int i = 0; i < count * step; i++) {
                long k = i;
                GetRaw getRaw = IndexUtil.get(acquired);
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {
                    byte[] expectedFP = desired.get(UIO.longBytes(key(rawEntry)));
                    if (expectedFP == null) {
                        Assert.assertTrue(expectedFP == null && value(rawEntry) == -1);
                    } else {
                        Assert.assertEquals(value(expectedFP), value(rawEntry));
                    }
                    return true;
                };

                while (getRaw.get(UIO.longBytes(k), stream)) ;
            }
            System.out.println("gets PASSED");
            return true;
        }, true);

        indexes.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            for (int i = 0; i < keys.size() - 3; i++) {
                int _i = i;

                int[] streamed = new int[1];
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {
                    if (value(rawEntry) > -1) {
                        System.out.println("Streamed:" + key(rawEntry));
                        streamed[0]++;
                    }
                    return true;
                };

                System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
                NextRawEntry rangeScan = IndexUtil.rangeScan(acquired, keys.get(_i), keys.get(_i + 3), rawhide);
                while (rangeScan.next(stream) == NextRawEntry.Next.more) ;
                Assert.assertEquals(3, streamed[0]);
            }

            System.out.println("rangeScan PASSED");
            return true;
        }, true);

        indexes.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            for (int i = 0; i < keys.size() - 3; i++) {
                int _i = i;
                int[] streamed = new int[1];
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {
                    if (value(rawEntry) > -1) {
                        streamed[0]++;
                    }
                    return true;
                };
                NextRawEntry rangeScan = IndexUtil.rangeScan(acquired, UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3), rawhide);
                while (rangeScan.next(stream) == NextRawEntry.Next.more) ;
                Assert.assertEquals(2, streamed[0]);

            }

            System.out.println("rangeScan2 PASSED");
            return true;
        }, true);
    }
}
