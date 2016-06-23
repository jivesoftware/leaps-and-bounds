package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;

/**
 *
 * @author jonathan.colt
 */
public class Gets implements GetRaw, RawEntryStream {

    private final ActiveScan activeScan;
    private RawEntryStream activeStream;
    private boolean found = false;
    private final byte[] intBuffer = new byte[4];

    public Gets(ActiveScan activeScan) {
        this.activeScan = activeScan;
    }

    @Override
    public boolean get(byte[] key, RawEntryStream stream) throws Exception {
        long activeFp = activeScan.getInclusiveStartOfRow(key, true, intBuffer);
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
    public boolean stream(RawEntryFormat rawEntryFormat, byte[] rawEntry, int offset, int length) throws Exception {
        boolean result = activeStream.stream(rawEntryFormat, rawEntry, offset, length);
        found = true;
        return result;
    }

    @Override
    public boolean result() {
        return activeScan.result();
    }

}
