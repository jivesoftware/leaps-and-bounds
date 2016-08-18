package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import java.nio.ByteBuffer;

/**
 *
 * @author jonathan.colt
 */
public class Gets implements GetRaw, RawEntryStream {

    private final ActiveScan activeScan;
    private RawEntryStream activeStream;
    private boolean found = false;

    public Gets(ActiveScan activeScan) {
        this.activeScan = activeScan;
    }

    @Override
    public boolean get(byte[] key, RawEntryStream stream) throws Exception {
        long activeFp = activeScan.getInclusiveStartOfRow(key, true);
        if (activeFp < 0) {
            return false;
        }
        activeStream = stream;
        found = false;
        activeScan.reset();
        boolean more = true;
        while (more && !found) {
            more = activeScan.next(activeFp, this);
        }
        return found;
    }

    @Override
    public boolean stream(FormatTransformer readKeyFormatTransformer,
        FormatTransformer readValueFormatTransformer,
        ByteBuffer rawEntry) throws Exception {
        boolean result = activeStream.stream(readKeyFormatTransformer, readValueFormatTransformer, rawEntry);
        found = true;
        return result;
    }

    @Override
    public boolean result() {
        return activeScan.result();
    }

}
