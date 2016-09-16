package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.rawhide.Rawhide;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.guts.api.Scanner;
import com.jivesoftware.os.lab.guts.api.Scanner.Next;
import com.jivesoftware.os.lab.io.BolBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

/**
 * @author jonathan.colt
 */
public class ReadLeapsAndBoundsIndex implements ReadIndex {

    private final Semaphore hideABone;
    private final Rawhide rawhide;
    private final Footer footer;
    private final Callable<ActiveScan> activeScan;

    public ReadLeapsAndBoundsIndex(Semaphore hideABone,
        Rawhide rawhide,
        Footer footer,
        Callable<ActiveScan> activeScan) {
        this.hideABone = hideABone;
        this.rawhide = rawhide;
        this.footer = footer;
        this.activeScan = activeScan;
    }

    @Override
    public String toString() {
        return "ReadLeapsAndBoundsIndex{" + "footer=" + footer + '}';
    }

    @Override
    public void release() {
        hideABone.release();
    }

    @Override
    public GetRaw get() throws Exception {
        return new Gets(activeScan.call());
    }

    private static final Scanner eos = new Scanner() {
        @Override
        public Next next(RawEntryStream stream) throws Exception {
            return Next.eos;
        }

        @Override
        public void close() {
        }
    };

    @Override
    public Scanner rangeScan(byte[] from, byte[] to, BolBuffer entryBuffer) throws Exception {

        BolBuffer bbFrom = from == null ? null : new BolBuffer(from);
        BolBuffer bbTo = to == null ? null : new BolBuffer(to);

        ActiveScan scan = activeScan.call();
        long fp = scan.getInclusiveStartOfRow(from, entryBuffer, false);
        if (fp < 0) {
            return eos;
        }
        return new Scanner() {
            @Override
            public Next next(RawEntryStream stream) throws Exception {
                BolBuffer entryBuffer = new BolBuffer();
                boolean[] once = new boolean[]{false};
                boolean more = true;
                while (!once[0] && more) {
                    more = scan.next(fp,
                        entryBuffer,
                        (readKeyFormatTransormer, readValueFormatTransormer, rawEntry) -> {
                            int c = rawhide.compareKey(readKeyFormatTransormer, readValueFormatTransormer, rawEntry, bbFrom);
                            if (c >= 0) {
                                c = to == null ? -1 : rawhide.compareKey(readKeyFormatTransormer, readValueFormatTransormer, rawEntry, bbTo);
                                if (c < 0) {
                                    once[0] = true;
                                }
                                return c < 0 && stream.stream(readKeyFormatTransormer, readValueFormatTransormer, rawEntry);
                            } else {
                                return true;
                            }
                        });
                }
                more = scan.result();
                return more ? Next.more : Next.stopped;
            }

            @Override
            public void close() {
            }
        };

    }

    @Override
    public Scanner rowScan(BolBuffer entryBuffer) throws Exception {
        ActiveScan scan = activeScan.call();
        return new Scanner() {
            @Override
            public Next next(RawEntryStream stream) throws Exception {
                scan.next(0, entryBuffer, stream);
                boolean more = scan.result();
                return more ? Next.more : Next.stopped;
            }

            @Override
            public void close() {
            }
        };

    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public long count() throws Exception {
        return footer.count;
    }

    @Override
    public boolean isEmpty() throws Exception {
        return footer.count == 0;
    }

}
