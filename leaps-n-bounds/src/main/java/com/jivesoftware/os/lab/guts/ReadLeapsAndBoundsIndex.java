package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.rawhide.Rawhide;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.guts.api.Scanner;
import com.jivesoftware.os.lab.io.BolBuffer;
import java.io.IOException;
import java.util.concurrent.Semaphore;

/**
 * @author jonathan.colt
 */
public class ReadLeapsAndBoundsIndex implements ReadIndex {

    private final Semaphore hideABone;
    private final Rawhide rawhide;
    private final Footer footer;
    private final ReadVisitor readVisitor;

    public ReadLeapsAndBoundsIndex(Semaphore hideABone,
        Rawhide rawhide,
        Footer footer,
        ReadVisitor readVisitor) {
        this.hideABone = hideABone;
        this.rawhide = rawhide;
        this.footer = footer;
        this.readVisitor = readVisitor;
    }

    public interface ReadVisitor {
        ActiveScan visit(ActiveScan activeScan) throws IOException;
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
        return readVisitor.visit(new ActiveScan());
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
    public Scanner rangeScan(byte[] from, byte[] to, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception {

        BolBuffer bbFrom = from == null ? null : new BolBuffer(from);
        BolBuffer bbTo = to == null ? null : new BolBuffer(to);

        ActiveScan scan = readVisitor.visit(new ActiveScan());
        long fp = scan.getInclusiveStartOfRow(from, entryBuffer, entryKeyBuffer, false);
        if (fp < 0) {
            return eos;
        }
        scan.setupAsRangeScanner(fp, to, entryBuffer, entryKeyBuffer, bbFrom, bbTo);
        return scan;

    }

    @Override
    public Scanner rowScan(BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception {
        ActiveScan scan = readVisitor.visit(new ActiveScan());
        scan.setupRowScan(entryBuffer,entryKeyBuffer);
        return scan;

    }

    @Override
    public long count() throws Exception {
        return footer.count;
    }
}
